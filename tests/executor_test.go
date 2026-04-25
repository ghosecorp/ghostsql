package tests

import (
	"os"
	"strings"
	"testing"

	"github.com/ghosecorp/ghostsql/internal/executor"
	"github.com/ghosecorp/ghostsql/internal/parser"
	"github.com/ghosecorp/ghostsql/internal/storage"
)

func TestFullDatabaseLifecycle(t *testing.T) {
	// Setup temporary data directory
	tmpDir := "./test_data_dir"
	os.MkdirAll(tmpDir, 0755)
	defer os.RemoveAll(tmpDir)

	db, err := storage.Initialize()
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	db.DataDir.RootPath = tmpDir

	// Create a session for the executor
	session := storage.NewSession("test-session")
	exec := executor.NewExecutor(db, session)
	
	// Create and switch to a test database
	dbName := "test_db"
	db.CreateDatabase(dbName)
	session.SetDatabase(dbName)

	tests := []struct {
		name          string
		query         string
		expected      string
		expectedError string
		checkRows     int
	}{
		{
			"Create Table",
			"CREATE TABLE users (id INT, name VARCHAR(50), age INT)",
			"CREATE TABLE users",
			"",
			0,
		},
		{
			"Insert Row 1",
			"INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)",
			"INSERT 1 row(s)",
			"",
			0,
		},
		{
			"Insert Row 2",
			"INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)",
			"INSERT 1 row(s)",
			"",
			0,
		},
		{
			"Select All",
			"SELECT * FROM users",
			"",
			"",
			2,
		},
		{
			"Select with WHERE",
			"SELECT * FROM users WHERE age > 28",
			"",
			"",
			1,
		},
		{
			"Create Vector Table",
			"CREATE TABLE embeddings (id INT, vec VECTOR(3))",
			"CREATE TABLE embeddings",
			"",
			0,
		},
		{
			"Insert Vector",
			"INSERT INTO embeddings (id, vec) VALUES (1, '[1.0, 0.0, 0.0]')",
			"INSERT 1 row(s)",
			"",
			0,
		},
		{
			"Vector Similarity Search (L2 Operator)",
			"SELECT * FROM embeddings ORDER BY vec <-> '[0.9, 0.1, 0.0]' LIMIT 1",
			"",
			"",
			1,
		},
		{
			"Vector Similarity Search (L2 Function)",
			"SELECT * FROM embeddings ORDER BY L2_DISTANCE(vec, '[0.9, 0.1, 0.0]') LIMIT 1",
			"",
			"",
			1,
		},
		{
			"Create Cosine Vector Table",
			"CREATE TABLE cosine_test (id INT, vec VECTOR(2))",
			"CREATE TABLE cosine_test",
			"",
			0,
		},
		{
			"Insert Cosine Vector 1",
			"INSERT INTO cosine_test (id, vec) VALUES (1, '[1.0, 0.0]')",
			"INSERT 1 row(s)",
			"",
			0,
		},
		{
			"Insert Cosine Vector 2",
			"INSERT INTO cosine_test (id, vec) VALUES (2, '[0.0, 1.0]')",
			"INSERT 1 row(s)",
			"",
			0,
		},
		{
			"Vector Similarity Search (Cosine Operator)",
			"SELECT * FROM cosine_test ORDER BY vec <=> '[0.9, 0.1]' LIMIT 1",
			"",
			"",
			1,
		},
		{
			"Vector Similarity Search (Cosine Function)",
			"SELECT * FROM cosine_test ORDER BY COSINE_DISTANCE(vec, '[0.9, 0.1]') LIMIT 1",
			"",
			"",
			1,
		},
		{
			"Create Departments Table",
			"CREATE TABLE depts (id INT PRIMARY KEY, name TEXT)",
			"CREATE TABLE depts",
			"",
			0,
		},
		{
			"Create Employees Table (No FK for Join Test)",
			"CREATE TABLE emps (id INT PRIMARY KEY, name TEXT, d_id INT)",
			"CREATE TABLE emps",
			"",
			0,
		},
		{
			"Insert Dept 1",
			"INSERT INTO depts (id, name) VALUES (1, 'Engineering')",
			"INSERT 1 row(s)",
			"",
			0,
		},
		{
			"Insert Another Dept",
			"INSERT INTO depts (id, name) VALUES (2, 'Sales')",
			"INSERT 1 row(s)",
			"",
			0,
		},
		{
			"Insert Dept without employees",
			"INSERT INTO depts (id, name) VALUES (3, 'Marketing')",
			"INSERT 1 row(s)",
			"",
			0,
		},
		{
			"Insert Emp 1",
			"INSERT INTO emps (id, name, d_id) VALUES (101, 'Alice', 1)",
			"INSERT 1 row(s)",
			"",
			0,
		},
		{
			"Insert Another Emp",
			"INSERT INTO emps (id, name, d_id) VALUES (102, 'Bob', 2)",
			"INSERT 1 row(s)",
			"",
			0,
		},
		{
			"Insert Emp without department",
			"INSERT INTO emps (id, name, d_id) VALUES (103, 'Charlie', 0)",
			"INSERT 1 row(s)",
			"",
			0,
		},
		{
			"Create FK Table for Failure Test",
			"CREATE TABLE emps_fk (id INT, d_id INT REFERENCES depts(id))",
			"CREATE TABLE emps_fk",
			"",
			0,
		},
		{
			"Foreign Key Failure Test",
			"INSERT INTO emps_fk (id, d_id) VALUES (1, 999)",
			"",
			"foreign key constraint failed",
			0,
		},
		{
			"Inner Join Test",
			"SELECT emps.name, depts.name FROM emps INNER JOIN depts ON emps.d_id = depts.id",
			"",
			"",
			2, // Alice (Eng), Bob (Sales)
		},
		{
			"Left Join Test",
			"SELECT emps.name, depts.name FROM emps LEFT JOIN depts ON emps.d_id = depts.id",
			"",
			"",
			3, // Alice, Bob, Charlie (null dept)
		},
		{
			"Right Join Test",
			"SELECT emps.name, depts.name FROM emps RIGHT JOIN depts ON emps.d_id = depts.id",
			"",
			"",
			3, // Eng, Sales, Marketing (null emp)
		},
		{
			"Full Join Test",
			"SELECT emps.name, depts.name FROM emps FULL JOIN depts ON emps.d_id = depts.id",
			"",
			"",
			4, // Alice, Bob, Charlie, Marketing
		},
		{
			"Cross Join Test",
			"SELECT emps.name, depts.name FROM emps CROSS JOIN depts",
			"",
			"",
			9, // 3 emps * 3 depts
		},
		{
			"System Catalog Discovery - pg_class",
			"SELECT relname FROM pg_catalog.pg_class WHERE relkind = 'r'",
			"",
			"",
			6, // users, embeddings, cosine_test, depts, emps, emps_fk
		},
		{
			"System Catalog Discovery - pg_namespace",
			"SELECT nspname FROM pg_catalog.pg_namespace",
			"",
			"",
			2, // public, pg_catalog
		},
		{
			"Update Row",
			"UPDATE users SET age = 31 WHERE name = 'Alice'",
			"UPDATE 1 row(s)",
			"",
			0,
		},
		{
			"Delete Row",
			"DELETE FROM users WHERE name = 'Bob'",
			"DELETE 1 row(s)",
			"",
			0,
		},
		{
			"Verify Update and Delete",
			"SELECT * FROM users",
			"",
			"",
			1, // Only Alice left
		},
		{
			"Alter Table Add Column",
			"ALTER TABLE users ADD email TEXT",
			"ALTER TABLE users ADD COLUMN email",
			"",
			0,
		},
		{
			"Truncate Table",
			"TRUNCATE TABLE users",
			"TRUNCATE TABLE users",
			"",
			0,
		},
		{
			"Verify Truncate",
			"SELECT * FROM users",
			"",
			"",
			0,
		},
		{
			"Drop Table",
			"DROP TABLE users",
			"DROP TABLE users",
			"",
			0,
		},
		{
			"Create Temp Database for Drop",
			"CREATE DATABASE temp_db",
			"CREATE DATABASE temp_db",
			"",
			0,
		},
		{
			"Drop Database",
			"DROP DATABASE temp_db",
			"DROP DATABASE temp_db",
			"",
			0,
		},
		{
			"Comment On Table (Metadata)",
			"COMMENT ON TABLE emps IS 'Employee records with department links'",
			"COMMENT ON TABLE emps",
			"",
			0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Logf("Running query: %s", tt.query)
			p := parser.NewParser(tt.query)
			stmt, err := p.Parse()
			if err != nil {
				t.Fatalf("Parse error for '%s': %v", tt.query, err)
			}

			res, err := exec.Execute(stmt)
			if tt.expectedError != "" {
				if err == nil {
					t.Fatalf("Expected error containing '%s', but got nil", tt.expectedError)
				}
				if !strings.Contains(err.Error(), tt.expectedError) {
					t.Fatalf("Expected error containing '%s', but got '%v'", tt.expectedError, err)
				}
				return // Test case passed as error was expected
			}

			if err != nil {
				t.Fatalf("Execute error for '%s': %v", tt.query, err)
			}

			if tt.expected != "" && res.Message != tt.expected {
				t.Errorf("Expected message '%s', got '%s'", tt.expected, res.Message)
			}

			if tt.checkRows > 0 && len(res.Rows) != tt.checkRows {
				t.Errorf("Expected %d rows, got %d. Result: %+v", tt.checkRows, len(res.Rows), res.Rows)
			}

			// For Join tests, verify column names are unique
			if strings.Contains(tt.name, "Join") && strings.HasPrefix(strings.ToUpper(tt.query), "SELECT") {
				if len(res.Columns) < 2 {
					t.Errorf("Expected at least 2 columns for JOIN, got %d", len(res.Columns))
				}
				// Check for duplicates in res.Columns
				seen := make(map[string]bool)
				for _, col := range res.Columns {
					if seen[col] {
						t.Errorf("Duplicate column name found in JOIN result: %s", col)
					}
					seen[col] = true
				}
			}
		})
	}
}
