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
