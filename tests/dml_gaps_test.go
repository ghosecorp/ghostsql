package tests

import (
	"os"
	"testing"

	"github.com/ghosecorp/ghostsql/internal/executor"
	"github.com/ghosecorp/ghostsql/internal/storage"
)

func TestDMLGaps(t *testing.T) {
	executor.ResetRegistries()
	tmpDir, err := os.MkdirTemp("", "ghostsql_dml_gaps")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	db, err := storage.Initialize(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Shutdown()

	session := db.SessionMgr.CreateSession("dml_gaps_session")
	session.SetUser("ghost")
	session.SetDatabase("ghostsql")
	exec := executor.NewExecutor(db, session)

	// Setup tables
	runQuery(t, exec, "CREATE TABLE source (id INT PRIMARY KEY, name TEXT)")
	runQuery(t, exec, "CREATE TABLE target (id INT PRIMARY KEY, title TEXT)")

	runQuery(t, exec, "INSERT INTO source (id, name) VALUES (1, 'alice')")
	runQuery(t, exec, "INSERT INTO source (id, name) VALUES (2, 'bob')")

	// 1. INSERT ... SELECT
	runQuery(t, exec, "INSERT INTO target SELECT id, name FROM source")
	res, err := exec.Execute(parseQuery("SELECT * FROM target ORDER BY id"))
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 rows inserted, got %d", len(res.Rows))
	}
	if res.Rows[0]["title"] != "alice" || res.Rows[1]["title"] != "bob" {
		t.Errorf("unexpected content: %v, %v", res.Rows[0], res.Rows[1])
	}

	// 2. INSERT ... ON CONFLICT (id) DO UPDATE/NOTHING (Upsert)
	runQuery(t, exec, "INSERT INTO target (id, title) VALUES (1, 'clara') ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title")
	res, err = exec.Execute(parseQuery("SELECT * FROM target WHERE id = 1"))
	if err != nil {
		t.Fatal(err)
	}
	if res.Rows[0]["title"] != "clara" {
		t.Errorf("expected clara after ON CONFLICT UPDATE, got %v", res.Rows[0]["title"])
	}

	// DO NOTHING
	runQuery(t, exec, "INSERT INTO target (id, title) VALUES (1, 'diana') ON CONFLICT (id) DO NOTHING")
	res, err = exec.Execute(parseQuery("SELECT * FROM target WHERE id = 1"))
	if err != nil {
		t.Fatal(err)
	}
	if res.Rows[0]["title"] != "clara" {
		t.Errorf("expected no change after DO NOTHING, got %v", res.Rows[0]["title"])
	}

	// 3. UPDATE ... FROM (Join-based Updates)
	runQuery(t, exec, "UPDATE target SET title = source.name FROM source WHERE target.id = source.id")
	res, err = exec.Execute(parseQuery("SELECT * FROM target ORDER BY id"))
	if err != nil {
		t.Fatal(err)
	}
	if res.Rows[0]["title"] != "alice" || res.Rows[1]["title"] != "bob" {
		t.Errorf("unexpected updated content: %v", res.Rows)
	}

	// 4. DELETE ... USING (Join-based Deletes)
	runQuery(t, exec, "DELETE FROM target USING source WHERE target.id = source.id AND source.name = 'bob'")
	res, err = exec.Execute(parseQuery("SELECT * FROM target"))
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row remaining, got %d: %v", len(res.Rows), res.Rows)
	}
	var remainingId int
	switch v := res.Rows[0]["id"].(type) {
	case int:
		remainingId = v
	case int64:
		remainingId = int(v)
	}
	if remainingId != 1 {
		t.Errorf("expected remaining ID to be 1, got %v", res.Rows[0]["id"])
	}

	// 5. MERGE INTO ... USING (Matched / Unmatched Actions)
	runQuery(t, exec, "CREATE TABLE staging (id INT, title TEXT)")
	runQuery(t, exec, "INSERT INTO staging (id, title) VALUES (1, 'alice_updated')")
	runQuery(t, exec, "INSERT INTO staging (id, title) VALUES (3, 'charlie')")

	runQuery(t, exec, "MERGE INTO target USING staging ON target.id = staging.id WHEN MATCHED THEN UPDATE SET title = staging.title WHEN NOT MATCHED THEN INSERT (id, title) VALUES (staging.id, staging.title)")

	res, err = exec.Execute(parseQuery("SELECT * FROM target ORDER BY id"))
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 rows after MERGE, got %d", len(res.Rows))
	}
	if res.Rows[0]["title"] != "alice_updated" || res.Rows[1]["title"] != "charlie" {
		t.Errorf("unexpected rows after MERGE: %v", res.Rows)
	}
}
