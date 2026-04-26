package tests

import (
	"os"
	"testing"

	"github.com/ghosecorp/ghostsql/internal/executor"
	"github.com/ghosecorp/ghostsql/internal/parser"
	"github.com/ghosecorp/ghostsql/internal/storage"
)

func TestRBAC(t *testing.T) {
	// Setup temporary data directory
	tmpDir, err := os.MkdirTemp("", "ghostsql_rbac_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	db, err := storage.Initialize(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Shutdown()

	// 1. Verify ghost superuser exists
	ghost, exists := db.RoleStore.GetRole("ghost")
	if !exists {
		t.Fatal("ghost role should exist")
	}
	if !ghost.IsSuperuser {
		t.Error("ghost should be superuser")
	}

	// 2. Create a new user (as ghost)
	ghostSession := db.SessionMgr.CreateSession("ghost_session")
	ghostSession.SetUser("ghost")
	ghostSession.SetDatabase("ghostsql")
	ghostExec := executor.NewExecutor(db, ghostSession)

	// Create table
	runQuery(t, ghostExec, "CREATE TABLE sensitive_data (id INT, secret TEXT)")
	runQuery(t, ghostExec, "INSERT INTO sensitive_data (id, secret) VALUES (1, 'top_secret')")

	// Create alice
	runQuery(t, ghostExec, "CREATE ROLE alice WITH LOGIN PASSWORD 'alice_pw'")

	// 3. Try to access as alice (should fail)
	aliceSession := db.SessionMgr.CreateSession("alice_session")
	aliceSession.SetUser("alice")
	aliceSession.SetDatabase("ghostsql")
	aliceExec := executor.NewExecutor(db, aliceSession)

	_, err = aliceExec.Execute(parseQuery("SELECT * FROM sensitive_data"))
	if err == nil {
		t.Error("alice should not be able to SELECT sensitive_data without GRANT")
	}

	// 4. Grant SELECT to alice
	runQuery(t, ghostExec, "GRANT SELECT ON TABLE sensitive_data TO alice")

	// 5. Try to access again (should succeed)
	res, err := aliceExec.Execute(parseQuery("SELECT * FROM sensitive_data"))
	if err != nil {
		t.Errorf("alice should be able to SELECT after GRANT: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(res.Rows))
	}

	// 6. Revoke SELECT
	runQuery(t, ghostExec, "REVOKE SELECT ON TABLE sensitive_data FROM alice")

	// 7. Try to access again (should fail)
	_, err = aliceExec.Execute(parseQuery("SELECT * FROM sensitive_data"))
	if err == nil {
		t.Error("alice should not be able to SELECT after REVOKE")
	}
}

func runQuery(t *testing.T, exec *executor.Executor, query string) {
	stmt, err := parser.NewParser(query).Parse()
	if err != nil {
		t.Fatalf("Failed to parse query '%s': %v", query, err)
	}
	_, err = exec.Execute(stmt)
	if err != nil {
		t.Fatalf("Failed to execute query '%s': %v", query, err)
	}
}

func parseQuery(query string) parser.Statement {
	stmt, _ := parser.NewParser(query).Parse()
	return stmt
}
