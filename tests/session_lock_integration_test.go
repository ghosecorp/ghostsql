package tests

import (
	"os"
	"strings"
	"testing"

	"github.com/ghosecorp/ghostsql/internal/executor"
	"github.com/ghosecorp/ghostsql/internal/parser"
	"github.com/ghosecorp/ghostsql/internal/storage"
)

func TestConcurrentBankTransfersWithRowLocks(t *testing.T) {
	dataDir := "test_data_bank_lock"
	_ = os.RemoveAll(dataDir)
	defer os.RemoveAll(dataDir)

	db, err := storage.Initialize(dataDir)
	if err != nil {
		t.Fatalf("Failed to initialize storage: %v", err)
	}
	defer func() {
		_ = db.Shutdown()
	}()

	sess1 := db.SessionMgr.CreateSession("bank_sess_1")
	sess1.SetUser("ghost")
	sess1.SetDatabase("ghostsql")
	exec1 := executor.NewExecutor(db, sess1)

	sess2 := db.SessionMgr.CreateSession("bank_sess_2")
	sess2.SetUser("ghost")
	sess2.SetDatabase("ghostsql")
	exec2 := executor.NewExecutor(db, sess2)

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

	parseQuery := func(q string) parser.Statement {
		p := parser.NewParser(q)
		stmt, err := p.Parse()
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		return stmt
	}

	// 1. Setup banking schema and tables
	runQuery(exec1, "CREATE TABLE accounts (id INT PRIMARY KEY, name TEXT, balance INT)")
	runQuery(exec1, "CREATE TABLE transfer_logs (id INT, log_type TEXT, amount INT)")

	runQuery(exec1, "INSERT INTO accounts (id, name, balance) VALUES (1, 'Alice', 1000)")
	runQuery(exec1, "INSERT INTO accounts (id, name, balance) VALUES (2, 'Bob', 500)")

	// 2. Set search paths and variables for each session
	runQuery(exec1, "SET search_path TO banking, public")
	runQuery(exec2, "SET search_path TO public")

	// 3. Start a concurrent lock scenario
	// Session 1 locks Alice's account with FOR UPDATE
	runQuery(exec1, "BEGIN")
	res := runQuery(exec1, "SELECT * FROM accounts WHERE id = 1 FOR UPDATE")
	if len(res.Rows) != 1 || res.Rows[0]["name"] != "Alice" {
		t.Errorf("Expected to select Alice, got: %v", res.Rows)
	}

	// Session 2 attempts to select Alice's account with FOR UPDATE -> must fail with locking error
	_, err = exec2.Execute(parseQuery("SELECT * FROM accounts WHERE id = 1 FOR UPDATE"))
	if err == nil || !strings.Contains(err.Error(), "locked by session bank_sess_1") {
		t.Errorf("Expected lock conflict error for Session 2, got: %v", err)
	}

	// Session 2 attempts to update Alice's account directly without lock -> must also fail due to table/row lock
	_, err = exec2.Execute(parseQuery("UPDATE accounts SET balance = 900 WHERE id = 1"))
	if err == nil || !strings.Contains(err.Error(), "locked by session bank_sess_1") {
		t.Errorf("Expected lock conflict error on update for Session 2, got: %v", err)
	}

	// 4. Session 1 processes transfer and uses savepoints
	runQuery(exec1, "UPDATE accounts SET balance = 800 WHERE id = 1")
	runQuery(exec1, "SAVEPOINT transfer_sp")

	// Session 1 performs an experimental insert in logs
	runQuery(exec1, "INSERT INTO transfer_logs (id, log_type, amount) VALUES (101, 'transfer', 200)")
	logRes := runQuery(exec1, "SELECT * FROM transfer_logs WHERE id = 101")
	if len(logRes.Rows) != 1 {
		t.Errorf("Expected 1 log row in Session 1, got: %v", logRes.Rows)
	}

	// Session 1 rolls back to savepoint to discard log row but keep account update and the lock!
	runQuery(exec1, "ROLLBACK TO transfer_sp")

	// Verify log row is discarded in Session 1
	logRes = runQuery(exec1, "SELECT * FROM transfer_logs WHERE id = 101")
	if len(logRes.Rows) != 0 {
		t.Errorf("Expected log row to be discarded after rollback to savepoint, got: %v", logRes.Rows)
	}

	// Verify Alice's balance remains 800 (1000 - 200) inside transaction
	res = runQuery(exec1, "SELECT balance FROM accounts WHERE id = 1")
	if len(res.Rows) != 1 || (res.Rows[0]["balance"] != 800 && res.Rows[0]["balance"] != 800.0) {
		t.Errorf("Expected balance to be 800, got: %v", res.Rows)
	}

	// 5. Session 1 commits, releasing lock
	runQuery(exec1, "COMMIT")

	// 6. Session 2 is now able to successfully execute and update Alice's account
	_, err = exec2.Execute(parseQuery("UPDATE accounts SET balance = 700 WHERE id = 1"))
	if err != nil {
		t.Errorf("Expected Session 2 update to succeed after lock release, got error: %v", err)
	}

	// Verify final balance is 700 (800 - 100)
	res = runQuery(exec2, "SELECT balance FROM accounts WHERE id = 1")
	if len(res.Rows) != 1 || (res.Rows[0]["balance"] != 700 && res.Rows[0]["balance"] != 700.0) {
		t.Errorf("Expected final balance to be 700, got: %v", res.Rows)
	}
}

func TestMultiUserImpersonationVariableRollback(t *testing.T) {
	dataDir := "test_data_multi_user"
	_ = os.RemoveAll(dataDir)
	defer os.RemoveAll(dataDir)

	db, err := storage.Initialize(dataDir)
	if err != nil {
		t.Fatalf("Failed to initialize storage: %v", err)
	}
	defer func() {
		_ = db.Shutdown()
	}()

	sess := db.SessionMgr.CreateSession("multi_user_sess")
	sess.SetUser("ghost")
	sess.SetDatabase("ghostsql")
	exec := executor.NewExecutor(db, sess)

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

	// 1. Verify initial user and defaults
	if sess.GetUser() != "ghost" {
		t.Errorf("Expected initial user 'ghost', got: %s", sess.GetUser())
	}

	runQuery("SET work_mem = '32MB'")
	res := runQuery("SHOW work_mem")
	if len(res.Rows) != 1 || res.Rows[0]["work_mem"] != "32MB" {
		t.Errorf("Expected work_mem to be 32MB, got: %v", res.Rows)
	}

	// 2. Perform transaction-scoped local updates
	runQuery("BEGIN")
	runQuery("SET LOCAL work_mem = '64MB'")
	runQuery("SET LOCAL timezone = 'EST'")

	// Verify inside transaction
	res = runQuery("SHOW work_mem")
	if len(res.Rows) != 1 || res.Rows[0]["work_mem"] != "64MB" {
		t.Errorf("Expected transaction-scoped work_mem to be 64MB, got: %v", res.Rows)
	}
	res = runQuery("SHOW timezone")
	if len(res.Rows) != 1 || res.Rows[0]["timezone"] != "EST" {
		t.Errorf("Expected transaction-scoped timezone to be EST, got: %v", res.Rows)
	}

	// 3. Impersonate another user inside the transaction
	runQuery("SET ROLE analyst")
	if sess.GetUser() != "analyst" {
		t.Errorf("Expected active role 'analyst', got: %s", sess.GetUser())
	}

	// 4. Rollback transaction and verify local variable rollback
	runQuery("ROLLBACK")

	// Transaction scoped work_mem and timezone must be restored
	res = runQuery("SHOW work_mem")
	if len(res.Rows) != 1 || res.Rows[0]["work_mem"] != "32MB" {
		t.Errorf("Expected work_mem to restore back to 32MB after rollback, got: %v", res.Rows)
	}

	res = runQuery("SHOW timezone")
	if len(res.Rows) != 1 || res.Rows[0]["timezone"] != "UTC" { // fallback to default UTC
		t.Errorf("Expected timezone to restore back to default 'UTC', got: %v", res.Rows)
	}
}
