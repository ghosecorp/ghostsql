package tests

import (
	"os"
	"testing"

	"github.com/ghosecorp/ghostsql/internal/executor"
	"github.com/ghosecorp/ghostsql/internal/parser"
	"github.com/ghosecorp/ghostsql/internal/storage"
)

func TestJSONAndJSONBFeatures(t *testing.T) {
	dataDir := "test_data_json"
	os.RemoveAll(dataDir)
	defer os.RemoveAll(dataDir)

	db, err := storage.Initialize(dataDir)
	if err != nil {
		t.Fatalf("Failed to initialize storage: %v", err)
	}

	session := db.SessionMgr.CreateSession("json_test")
	session.SetUser("ghost")
	session.SetDatabase("ghostsql")

	exec := executor.NewExecutor(db, session)

	// Create table with JSONB column
	queries := []string{
		"CREATE TABLE users (id INT, profile JSONB)",
		"INSERT INTO users (id, profile) VALUES (1, '{\"name\": \"Alice\", \"age\": 30, \"tags\": [\"admin\", \"user\"], \"meta\": {\"login\": \"alice123\"}}')",
		"INSERT INTO users (id, profile) VALUES (2, '{\"name\": \"Bob\", \"age\": 25, \"tags\": [\"user\"]}')",
	}

	for _, q := range queries {
		p := parser.NewParser(q)
		stmt, err := p.Parse()
		if err != nil {
			t.Fatalf("Failed to parse query %s: %v", q, err)
		}
		_, err = exec.Execute(stmt)
		if err != nil {
			t.Fatalf("Failed to execute query %s: %v", q, err)
		}
	}

	// 1. Test JSONB arrow extraction operator (->>)
	t.Run("JSONB_Text_Extraction", func(t *testing.T) {
		q := "SELECT profile->>'name' FROM users WHERE id = 1"
		p := parser.NewParser(q)
		stmt, err := p.Parse()
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		res, err := exec.Execute(stmt)
		if err != nil {
			t.Fatalf("Failed to execute: %v", err)
		}
		if len(res.Rows) != 1 {
			t.Fatalf("Expected 1 row, got %d", len(res.Rows))
		}
		val := res.Rows[0]["profile->>'name'"]
		if val != "Alice" {
			t.Errorf("Expected 'Alice', got %v", val)
		}
	})

	// 2. Test JSONB nested object/array extraction (->)
	t.Run("JSONB_Object_Extraction", func(t *testing.T) {
		q := "SELECT profile->'meta' FROM users WHERE id = 1"
		p := parser.NewParser(q)
		stmt, err := p.Parse()
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		res, err := exec.Execute(stmt)
		if err != nil {
			t.Fatalf("Failed to execute: %v", err)
		}
		if len(res.Rows) != 1 {
			t.Fatalf("Expected 1 row, got %d", len(res.Rows))
		}
		val := res.Rows[0]["profile->'meta'"]
		expected := `{"login":"alice123"}`
		if val != expected {
			t.Errorf("Expected '%s', got %v", expected, val)
		}
	})

	// 3. Test JSONB containment operator (@>)
	t.Run("JSONB_Containment_Operator", func(t *testing.T) {
		q := "SELECT id FROM users WHERE profile @> '{\"name\": \"Bob\"}'"
		p := parser.NewParser(q)
		stmt, err := p.Parse()
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		res, err := exec.Execute(stmt)
		if err != nil {
			t.Fatalf("Failed to execute: %v", err)
		}
		if len(res.Rows) != 1 {
			t.Fatalf("Expected 1 row, got %d", len(res.Rows))
		}
		if res.Rows[0]["id"] != 2.0 && res.Rows[0]["id"] != 2 {
			t.Errorf("Expected ID 2, got %v", res.Rows[0]["id"])
		}
	})

	// 4. Test jsonb_path_query function
	t.Run("jsonb_path_query", func(t *testing.T) {
		q := "SELECT jsonb_path_query(profile, '$.meta.login') AS login_val FROM users WHERE id = 1"
		p := parser.NewParser(q)
		stmt, err := p.Parse()
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		res, err := exec.Execute(stmt)
		if err != nil {
			t.Fatalf("Failed to execute: %v", err)
		}
		if len(res.Rows) != 1 {
			t.Fatalf("Expected 1 row, got %d. Rows: %v", len(res.Rows), res.Rows)
		}
		val := res.Rows[0]["login_val"]
		if val != "alice123" {
			t.Errorf("Expected 'alice123', got %v. Row content: %v", val, res.Rows[0])
		}
	})
}

func TestTCLTransactions(t *testing.T) {
	dataDir := "test_data_tcl"
	os.RemoveAll(dataDir)
	defer os.RemoveAll(dataDir)

	db, err := storage.Initialize(dataDir)
	if err != nil {
		t.Fatalf("Failed to initialize storage: %v", err)
	}

	session1 := db.SessionMgr.CreateSession("session1")
	session1.SetUser("ghost")
	session1.SetDatabase("ghostsql")

	session2 := db.SessionMgr.CreateSession("session2")
	session2.SetUser("ghost")
	session2.SetDatabase("ghostsql")

	exec1 := executor.NewExecutor(db, session1)
	exec2 := executor.NewExecutor(db, session2)

	// Helper to execute query and expect success
	runQuery := func(exec *executor.Executor, q string) *executor.Result {
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

	// 1. Setup base table
	runQuery(exec1, "CREATE TABLE accounts (id INT, balance INT)")
	runQuery(exec1, "INSERT INTO accounts (id, balance) VALUES (1, 100)")

	// 2. Start a transaction on Session 1 and modify data
	runQuery(exec1, "BEGIN")
	runQuery(exec1, "UPDATE accounts SET balance = 200 WHERE id = 1")

	// 3. Verify Session 1 sees the uncommitted update
	res1 := runQuery(exec1, "SELECT balance FROM accounts WHERE id = 1")
	if len(res1.Rows) != 1 || (res1.Rows[0]["balance"] != 200 && res1.Rows[0]["balance"] != 200.0) {
		t.Errorf("Session 1 expected balance 200, got: %v", res1.Rows)
	}

	// 4. Verify Session 2 does NOT see the uncommitted update (ACID isolation!)
	res2 := runQuery(exec2, "SELECT balance FROM accounts WHERE id = 1")
	if len(res2.Rows) != 1 || (res2.Rows[0]["balance"] != 100 && res2.Rows[0]["balance"] != 100.0) {
		t.Errorf("Session 2 expected balance 100, got: %v", res2.Rows)
	}

	// 5. Rollback on Session 1
	runQuery(exec1, "ROLLBACK")

	// 6. Verify Session 1 sees the rolled back state (100)
	res1 = runQuery(exec1, "SELECT balance FROM accounts WHERE id = 1")
	if len(res1.Rows) != 1 || (res1.Rows[0]["balance"] != 100 && res1.Rows[0]["balance"] != 100.0) {
		t.Errorf("Session 1 expected rolled back balance 100, got: %v", res1.Rows)
	}

	// 7. Start another transaction, update, and COMMIT
	runQuery(exec1, "BEGIN")
	runQuery(exec1, "UPDATE accounts SET balance = 300 WHERE id = 1")
	runQuery(exec1, "COMMIT")

	// 8. Verify Session 2 now sees the committed update (300)
	res2 = runQuery(exec2, "SELECT balance FROM accounts WHERE id = 1")
	if len(res2.Rows) != 1 || (res2.Rows[0]["balance"] != 300 && res2.Rows[0]["balance"] != 300.0) {
		t.Errorf("Session 2 expected committed balance 300, got: %v", res2.Rows)
	}

	// 9. Test SAVEPOINT and ROLLBACK TO SAVEPOINT
	runQuery(exec1, "BEGIN")
	runQuery(exec1, "UPDATE accounts SET balance = 400 WHERE id = 1")
	runQuery(exec1, "SAVEPOINT sp1")
	runQuery(exec1, "UPDATE accounts SET balance = 500 WHERE id = 1")

	// 9.1 Verify balance is 500 inside transaction
	res1 = runQuery(exec1, "SELECT balance FROM accounts WHERE id = 1")
	if len(res1.Rows) != 1 || (res1.Rows[0]["balance"] != 500 && res1.Rows[0]["balance"] != 500.0) {
		t.Errorf("Expected balance 500 before rollback to savepoint, got: %v", res1.Rows)
	}

	// 9.2 Rollback to savepoint sp1
	runQuery(exec1, "ROLLBACK TO sp1")

	// 9.3 Verify balance restored to 400 inside transaction
	res1 = runQuery(exec1, "SELECT balance FROM accounts WHERE id = 1")
	if len(res1.Rows) != 1 || (res1.Rows[0]["balance"] != 400 && res1.Rows[0]["balance"] != 400.0) {
		t.Errorf("Expected balance 400 after rollback to savepoint, got: %v", res1.Rows)
	}

	// 9.4 Commit the transaction
	runQuery(exec1, "COMMIT")

	// 9.5 Verify balance is committed at 400
	res2 = runQuery(exec2, "SELECT balance FROM accounts WHERE id = 1")
	if len(res2.Rows) != 1 || (res2.Rows[0]["balance"] != 400 && res2.Rows[0]["balance"] != 400.0) {
		t.Errorf("Expected committed balance 400, got: %v", res2.Rows)
	}
}
