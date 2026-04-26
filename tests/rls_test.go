package tests

import (
	"os"
	"testing"

	"github.com/ghosecorp/ghostsql/internal/executor"
	"github.com/ghosecorp/ghostsql/internal/parser"
	"github.com/ghosecorp/ghostsql/internal/storage"
)

func TestRowLevelSecurity(t *testing.T) {
	dataDir := "test_data_rls"
	os.RemoveAll(dataDir)
	defer os.RemoveAll(dataDir)

	db, err := storage.Initialize(dataDir)
	if err != nil {
		t.Fatalf("Failed to initialize storage: %v", err)
	}

	// Create a default session for setup
	session := db.SessionMgr.CreateSession("setup")
	session.SetUser("ghost")
	session.SetDatabase("ghostsql")

	exec := executor.NewExecutor(db, session)

	// 1. Setup users and table
	queries := []string{
		"CREATE ROLE alice WITH LOGIN PASSWORD 'pass'",
		"CREATE ROLE bob WITH LOGIN PASSWORD 'pass'",
		"CREATE TABLE secrets (id INT, owner VARCHAR(50), data TEXT)",
		"INSERT INTO secrets (id, owner, data) VALUES (1, 'alice', 'Alice private note')",
		"INSERT INTO secrets (id, owner, data) VALUES (2, 'bob', 'Bob private note')",
		"INSERT INTO secrets (id, owner, data) VALUES (3, 'alice', 'Another Alice note')",
		"GRANT SELECT ON secrets TO all",
		"ALTER TABLE secrets ENABLE ROW LEVEL SECURITY",
		"CREATE POLICY own_secrets ON secrets FOR SELECT TO all USING (owner = current_user())",
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

	// 2. Test as Alice
	aliceSession := db.SessionMgr.CreateSession("alice_session")
	aliceSession.SetUser("alice")
	aliceSession.SetDatabase("ghostsql")
	exec.SetSession(aliceSession)

	q := "SELECT * FROM secrets"
	p := parser.NewParser(q)
	stmt, _ := p.Parse()
	res, err := exec.Execute(stmt)
	if err != nil {
		t.Fatalf("Alice failed to select: %v", err)
	}

	if len(res.Rows) != 2 {
		t.Errorf("Alice expected 2 rows, got %d", len(res.Rows))
	}
	for _, row := range res.Rows {
		if row["owner"] != "alice" {
			t.Errorf("Alice saw row owned by %v", row["owner"])
		}
	}

	// 3. Test as Bob
	bobSession := db.SessionMgr.CreateSession("bob_session")
	bobSession.SetUser("bob")
	bobSession.SetDatabase("ghostsql")
	exec.SetSession(bobSession)

	res, err = exec.Execute(stmt)
	if err != nil {
		t.Fatalf("Bob failed to select: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Errorf("Bob expected 1 row, got %d", len(res.Rows))
	}
	if res.Rows[0]["owner"] != "bob" {
		t.Errorf("Bob saw row owned by %v", res.Rows[0]["owner"])
	}

	// 4. Test as Ghost (Superuser - bypasses RLS)
	exec.SetSession(session) // Back to ghost
	res, err = exec.Execute(stmt)
	if err != nil {
		t.Fatalf("Ghost failed to select: %v", err)
	}
	if len(res.Rows) != 3 {
		t.Errorf("Ghost (superuser) expected 3 rows, got %d", len(res.Rows))
	}
}
