package tests

import (
	"fmt"
	"os"
	"testing"

	"github.com/ghosecorp/ghostsql/internal/executor"
	"github.com/ghosecorp/ghostsql/internal/parser"
	"github.com/ghosecorp/ghostsql/internal/storage"
)

func setupAdvancedDQLTestEngine(t *testing.T) (*storage.Database, string) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()

	db, err := storage.Initialize(tempDir)
	if err != nil {
		t.Fatalf("Failed to initialize storage: %v", err)
	}

	session := storage.NewSession("test-session")
	session.SetUser("ghost")
	db.CreateDatabase("ghostsql")
	session.SetDatabase("ghostsql")

	// Set up initial data
	exec := executor.NewExecutor(db, session)
	
	setupQueries := []string{
		"CREATE TABLE adv_users (id INT, name TEXT, age INT, joined_date DATE)",
		"INSERT INTO adv_users (id, name, age, joined_date) VALUES (1, 'Alice', 30, '2023-01-15')",
		"INSERT INTO adv_users (id, name, age, joined_date) VALUES (2, 'Bob', 25, '2023-02-20')",
		"INSERT INTO adv_users (id, name, age, joined_date) VALUES (3, 'Charlie', 35, '2023-03-10')",
		
		"CREATE TABLE adv_orders (id INT, user_id INT, amount FLOAT)",
		"INSERT INTO adv_orders (id, user_id, amount) VALUES (101, 1, 150.5)",
		"INSERT INTO adv_orders (id, user_id, amount) VALUES (102, 1, 200.0)",
		"INSERT INTO adv_orders (id, user_id, amount) VALUES (103, 2, 50.0)",
	}

	for _, q := range setupQueries {
		p := parser.NewParser(q)
		stmt, err := p.Parse()
		if err != nil {
			t.Fatalf("Failed to parse setup query '%s': %v", q, err)
		}
		_, err = exec.Execute(stmt)
		if err != nil {
			t.Fatalf("Failed to execute setup query '%s': %v", q, err)
		}
	}

	return db, tempDir
}

func runAdvancedQuery(t *testing.T, exec *executor.Executor, query string) *executor.Result {
	p := parser.NewParser(query)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Failed to parse query '%s': %v", query, err)
	}
	result, err := exec.Execute(stmt)
	if err != nil {
		t.Fatalf("Failed to execute query '%s': %v", query, err)
	}
	return result
}

func TestDQLAdvancedFeatures(t *testing.T) {
	db, tempDir := setupAdvancedDQLTestEngine(t)
	defer os.RemoveAll(tempDir)
	session := storage.NewSession("test-session")
	session.SetUser("ghost")
	session.SetDatabase("ghostsql")
	exec := executor.NewExecutor(db, session)

	t.Run("Scalar Subqueries", func(t *testing.T) {
		res := runAdvancedQuery(t, exec, "SELECT name, (SELECT amount FROM adv_orders WHERE user_id = adv_users.id LIMIT 1) AS first_order FROM adv_users WHERE id = 1")
		if len(res.Rows) != 1 {
			t.Errorf("Expected 1 row, got %d", len(res.Rows))
		}
		if res.Rows[0]["first_order"] != 150.5 {
			t.Errorf("Expected first_order to be 150.5, got %v", res.Rows[0]["first_order"])
		}
	})

	t.Run("Recursive CTEs", func(t *testing.T) {
		// Note: Our simple executor might evaluate things slightly differently but it should terminate and yield rows
		query := `WITH RECURSIVE cnt AS (
			SELECT 1 AS n
			UNION ALL
			SELECT n + 1 FROM cnt WHERE n < 5
		) SELECT * FROM cnt`
		res := runAdvancedQuery(t, exec, query)
		if len(res.Rows) != 5 {
			t.Errorf("Expected 5 rows from recursive CTE, got %d", len(res.Rows))
		}
	})

	t.Run("Type Casting (::)", func(t *testing.T) {
		res := runAdvancedQuery(t, exec, "SELECT '123'::INT AS num, '2023-01-01'::DATE AS d FROM adv_users LIMIT 1")
		if len(res.Rows) != 1 {
			t.Errorf("Expected 1 row, got %d", len(res.Rows))
		}
		if res.Rows[0]["num"] != 123 {
			t.Errorf("Expected num to be 123, got %v", res.Rows[0]["num"])
		}
		if res.Rows[0]["d"] != "2023-01-01" {
			t.Errorf("Expected d to be '2023-01-01', got %v", res.Rows[0]["d"])
		}
	})

	t.Run("Column Aliases in WHERE", func(t *testing.T) {
		res := runAdvancedQuery(t, exec, "SELECT age + 10 AS future_age FROM adv_users WHERE future_age > 40")
		if len(res.Rows) != 1 {
			t.Errorf("Expected 1 row, got %d", len(res.Rows))
		}
		if res.Rows[0]["future_age"] != 45.0 && res.Rows[0]["future_age"] != 45 {
			t.Errorf("Expected future_age to be 45, got %v", res.Rows[0]["future_age"])
		}
	})

	t.Run("Advanced Date Functions", func(t *testing.T) {
		res := runAdvancedQuery(t, exec, "SELECT EXTRACT(YEAR FROM '2024-05-15 10:30:00'::TIMESTAMP) AS y, DATE_TRUNC('month', '2024-05-15 10:30:00'::TIMESTAMP) AS t FROM adv_users LIMIT 1")
		if len(res.Rows) != 1 {
			t.Errorf("Expected 1 row, got %d", len(res.Rows))
		}
		if res.Rows[0]["y"] != 2024.0 {
			t.Errorf("Expected year to be 2024.0, got %v", res.Rows[0]["y"])
		}
		
		// Expected RFC3339 format, we check prefix
		t_str := fmt.Sprintf("%v", res.Rows[0]["t"])
		if t_str[:10] != "2024-05-01" {
			t.Errorf("Expected truncated date to start with '2024-05-01', got %s", t_str)
		}
	})

	t.Run("LATERAL Joins", func(t *testing.T) {
		query := `SELECT u.name, o.amount 
		FROM adv_users u 
		CROSS JOIN LATERAL (SELECT amount FROM adv_orders WHERE user_id = u.id LIMIT 1) o`
		res := runAdvancedQuery(t, exec, query)
		
		// Only Alice and Bob have orders
		if len(res.Rows) != 2 {
			t.Errorf("Expected 2 rows from LATERAL join, got %d", len(res.Rows))
		}
	})
}
