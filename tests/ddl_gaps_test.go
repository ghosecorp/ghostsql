package tests

import (
	"os"
	"testing"

	"github.com/ghosecorp/ghostsql/internal/executor"
	"github.com/ghosecorp/ghostsql/internal/storage"
)

func TestDDLGaps(t *testing.T) {
	executor.ResetRegistries()
	tmpDir, err := os.MkdirTemp("", "ghostsql_ddl_gaps")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	db, err := storage.Initialize(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Shutdown()

	session := db.SessionMgr.CreateSession("ddl_gaps_session")
	session.SetUser("ghost")
	session.SetDatabase("ghostsql")
	exec := executor.NewExecutor(db, session)

	// 1. CREATE SCHEMA
	runQuery(t, exec, "CREATE SCHEMA IF NOT EXISTS billing")

	// 2. CREATE TYPE (ENUM)
	runQuery(t, exec, "CREATE TYPE status_enum AS ENUM ('active', 'inactive', 'pending')")

	// 3. CREATE SEQUENCE & default sequences ('SERIAL' shorthand)
	runQuery(t, exec, "CREATE SEQUENCE user_seq START WITH 100 INCREMENT BY 5")
	runQuery(t, exec, "CREATE TABLE users (id INT PRIMARY KEY DEFAULT nextval('user_seq'), name TEXT)")
	runQuery(t, exec, "INSERT INTO users (name) VALUES ('alice')")
	runQuery(t, exec, "INSERT INTO users (name) VALUES ('bob')")

	// Verify IDs from sequence increments
	res, err := exec.Execute(parseQuery("SELECT * FROM users ORDER BY id"))
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(res.Rows))
	}
	
	var id1, id2 int
	switch v := res.Rows[0]["id"].(type) {
	case int:
		id1 = v
	case int64:
		id1 = int(v)
	}
	switch v := res.Rows[1]["id"].(type) {
	case int:
		id2 = v
	case int64:
		id2 = int(v)
	}

	if id1 != 100 || id2 != 105 {
		t.Errorf("unexpected IDs: %v, %v", id1, id2)
	}

	// 4. CREATE VIEW & DROP VIEW
	runQuery(t, exec, "CREATE VIEW active_users AS SELECT id, name FROM users WHERE id = 100")
	res, err = exec.Execute(parseQuery("SELECT * FROM active_users"))
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row from view active_users, got %d", len(res.Rows))
	}
	if res.Rows[0]["name"] != "alice" {
		t.Errorf("expected alice, got %v", res.Rows[0]["name"])
	}

	runQuery(t, exec, "DROP VIEW IF EXISTS active_users")

	// 5. CREATE MATERIALIZED VIEW & REFRESH MATERIALIZED VIEW
	runQuery(t, exec, "CREATE MATERIALIZED VIEW mv_users AS SELECT id, name FROM users")
	res, err = exec.Execute(parseQuery("SELECT * FROM mv_users"))
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 rows from materialized view, got %d", len(res.Rows))
	}

	// Insert into physical source table users, verify MV doesn't update automatically
	runQuery(t, exec, "INSERT INTO users (name) VALUES ('charlie')")
	res, err = exec.Execute(parseQuery("SELECT * FROM mv_users"))
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("MV should remain size 2 before refresh, got %d", len(res.Rows))
	}

	// Refresh materialized view, verify updates
	runQuery(t, exec, "REFRESH MATERIALIZED VIEW mv_users")
	res, err = exec.Execute(parseQuery("SELECT * FROM mv_users"))
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 3 {
		t.Fatalf("expected 3 rows after MV refresh, got %d", len(res.Rows))
	}

	// 6. ALTER TABLE PHYSICAL STORAGE MUTATIONS
	// A. ADD COLUMN
	runQuery(t, exec, "ALTER TABLE users ADD COLUMN age INT")
	// B. DROP COLUMN
	runQuery(t, exec, "ALTER TABLE users DROP COLUMN IF EXISTS age")
	// C. RENAME COLUMN
	runQuery(t, exec, "ALTER TABLE users RENAME COLUMN name TO username")
	// D. RENAME TO
	runQuery(t, exec, "ALTER TABLE users RENAME TO member")
	// E. ALTER COLUMN TYPE
	runQuery(t, exec, "ALTER TABLE member ALTER COLUMN id TYPE BIGINT")

	// Verify all structural alterations together
	res, err = exec.Execute(parseQuery("SELECT * FROM member"))
	if err != nil {
		t.Fatal(err)
	}
	// Verify "name" column was renamed to "username", and "age" was dropped
	if len(res.Columns) != 2 {
		t.Fatalf("expected 2 columns left, got %d: %v", len(res.Columns), res.Columns)
	}
	if res.Columns[0] != "id" || res.Columns[1] != "username" {
		t.Errorf("unexpected columns remaining: %v", res.Columns)
	}
}
