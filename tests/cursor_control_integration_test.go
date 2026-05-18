package tests

import (
	"os"
	"testing"

	"github.com/ghosecorp/ghostsql/internal/executor"
	"github.com/ghosecorp/ghostsql/internal/parser"
	"github.com/ghosecorp/ghostsql/internal/storage"
)

func TestCursorControlLifecycle(t *testing.T) {
	dataDir := "test_data_cursor"
	_ = os.RemoveAll(dataDir)
	defer os.RemoveAll(dataDir)

	db, err := storage.Initialize(dataDir)
	if err != nil {
		t.Fatalf("Failed to initialize storage: %v", err)
	}
	defer func() {
		_ = db.Shutdown()
	}()

	session := db.SessionMgr.CreateSession("cursor_session")
	session.SetUser("ghost")
	session.SetDatabase("ghostsql")
	exec := executor.NewExecutor(db, session)

	runQuery := func(q string) *executor.Result {
		p := parser.NewParser(q)
		stmt, err := p.Parse()
		if err != nil {
			t.Fatalf("Failed to parse query '%s': %v", q, err)
		}
		res, err := exec.Execute(stmt)
		if err != nil {
			t.Fatalf("Failed to execute query '%s': %v", q, err)
		}
		return res
	}

	parseQuery := func(q string) parser.Statement {
		p := parser.NewParser(q)
		stmt, err := p.Parse()
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		return stmt
	}

	// 1. Setup table and items
	runQuery("CREATE TABLE items (id INT, name TEXT)")
	runQuery("INSERT INTO items (id, name) VALUES (1, 'apple')")
	runQuery("INSERT INTO items (id, name) VALUES (2, 'banana')")
	runQuery("INSERT INTO items (id, name) VALUES (3, 'cherry')")
	runQuery("INSERT INTO items (id, name) VALUES (4, 'date')")
	runQuery("INSERT INTO items (id, name) VALUES (5, 'elderberry')")

	// 2. DECLARE cursor
	runQuery("DECLARE my_cursor CURSOR FOR SELECT * FROM items")

	// 3. FETCH NEXT
	res := runQuery("FETCH NEXT FROM my_cursor")
	if len(res.Rows) != 1 || res.Rows[0]["name"] != "apple" {
		t.Errorf("Expected first fetched item to be 'apple', got: %v", res.Rows)
	}

	// 4. FETCH FORWARD 2
	res = runQuery("FETCH FORWARD 2 FROM my_cursor")
	if len(res.Rows) != 2 || res.Rows[0]["name"] != "banana" || res.Rows[1]["name"] != "cherry" {
		t.Errorf("Expected FETCH FORWARD 2 to return 'banana' and 'cherry', got: %v", res.Rows)
	}

	// 5. MOVE FORWARD 1
	moveRes := runQuery("MOVE FORWARD 1 IN my_cursor")
	if moveRes.Message != "MOVE 1" {
		t.Errorf("Expected MOVE 1, got: %s", moveRes.Message)
	}

	// 6. FETCH remaining (only elderberry left)
	res = runQuery("FETCH NEXT FROM my_cursor")
	if len(res.Rows) != 1 || res.Rows[0]["name"] != "elderberry" {
		t.Errorf("Expected final item to be 'elderberry', got: %v", res.Rows)
	}

	// 7. FETCH past end
	res = runQuery("FETCH NEXT FROM my_cursor")
	if len(res.Rows) != 0 {
		t.Errorf("Expected FETCH past end to return 0 rows, got: %v", res.Rows)
	}

	// 8. CLOSE cursor
	runQuery("CLOSE my_cursor")

	// 9. Verify FETCH fails post-CLOSE
	_, err = exec.Execute(parseQuery("FETCH NEXT FROM my_cursor"))
	if err == nil {
		t.Errorf("Expected FETCH on closed cursor to fail, but it succeeded")
	}
}

func TestDCLOwnerPrivilegeRetention(t *testing.T) {
	dataDir := "test_data_dcl_owner"
	_ = os.RemoveAll(dataDir)
	defer os.RemoveAll(dataDir)

	db, err := storage.Initialize(dataDir)
	if err != nil {
		t.Fatalf("Failed to initialize storage: %v", err)
	}
	defer func() {
		_ = db.Shutdown()
	}()

	// 1. Create sessions for ghost (superuser), alice (table owner), and bob (restricted)
	ghostSess := db.SessionMgr.CreateSession("ghost_session")
	ghostSess.SetUser("ghost")
	ghostSess.SetDatabase("ghostsql")
	ghostExec := executor.NewExecutor(db, ghostSess)

	// Create role alice and grant database-level CREATE privilege
	pRole := parser.NewParser("CREATE ROLE alice")
	stmtRole, _ := pRole.Parse()
	_, _ = ghostExec.Execute(stmtRole)

	pGrant := parser.NewParser("GRANT CREATE ON DATABASE ghostsql TO alice")
	stmtGrant, _ := pGrant.Parse()
	_, _ = ghostExec.Execute(stmtGrant)

	aliceSess := db.SessionMgr.CreateSession("alice_session")
	aliceSess.SetUser("alice")
	aliceSess.SetDatabase("ghostsql")
	aliceExec := executor.NewExecutor(db, aliceSess)

	// Create table owned by Alice (standard user)
	p := parser.NewParser("CREATE TABLE secure_data (id INT, secret TEXT)")
	stmt, _ := p.Parse()
	_, err = aliceExec.Execute(stmt)
	if err != nil {
		t.Fatalf("Alice failed to create table: %v", err)
	}

	// Insert test data as Alice
	p = parser.NewParser("INSERT INTO secure_data (id, secret) VALUES (1, 'alice_key')")
	stmt, _ = p.Parse()
	_, _ = aliceExec.Execute(stmt)

	// Verify Alice (owner) has SELECT access
	p = parser.NewParser("SELECT * FROM secure_data")
	stmt, _ = p.Parse()
	res, err := aliceExec.Execute(stmt)
	if err != nil || len(res.Rows) != 1 {
		t.Errorf("Alice expected to SELECT her own table, got: error=%v, rows=%v", err, res.Rows)
	}

	// 2. Superuser revokes SELECT privilege from Alice on her own table
	p = parser.NewParser("REVOKE SELECT ON TABLE secure_data FROM alice")
	stmt, _ = p.Parse()
	_, err = ghostExec.Execute(stmt)
	if err != nil {
		t.Fatalf("Failed to execute REVOKE: %v", err)
	}

	// 3. CRITICAL TEST: Owner privilege retention standard
	// Verify that Alice (owner) STILL retains access to SELECT even after explicit REVOKE
	p = parser.NewParser("SELECT * FROM secure_data")
	stmt, _ = p.Parse()
	res, err = aliceExec.Execute(stmt)
	if err != nil {
		t.Errorf("FAIL: Table owner Alice lost SELECT privilege after REVOKE (owner should always retain access). Error: %v", err)
	} else if len(res.Rows) != 1 || res.Rows[0]["secret"] != "alice_key" {
		t.Errorf("Expected 1 secure row, got: %v", res.Rows)
	}
}
