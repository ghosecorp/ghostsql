package tests

import (
	"os"
	"testing"

	"github.com/ghosecorp/ghostsql/internal/executor"
	"github.com/ghosecorp/ghostsql/internal/parser"
	"github.com/ghosecorp/ghostsql/internal/storage"
)

func TestComprehensiveQueries(t *testing.T) {
	// Setup temporary data directory
	tmpDir := "./test_comprehensive_dir"
	os.MkdirAll(tmpDir, 0755)
	defer os.RemoveAll(tmpDir)

	db, err := storage.Initialize(tmpDir)
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}

	// Create a session for the executor
	session := storage.NewSession("test-comprehensive-session")
	session.SetUser("ghost")
	exec := executor.NewExecutor(db, session)

	// Create and switch to a test database
	dbName := "comprehensive_db"
	db.CreateDatabase(dbName)
	session.SetDatabase(dbName)

	// Helper to run query
	run := func(t *testing.T, query string) *executor.Result {
		p := parser.NewParser(query)
		stmt, err := p.Parse()
		if err != nil {
			t.Fatalf("Parse error for '%s': %v", query, err)
		}
		res, err := exec.Execute(stmt)
		if err != nil {
			t.Fatalf("Execute error for '%s': %v", query, err)
		}
		return res
	}

	// 1. Setup tables
	run(t, "CREATE TABLE depts (id INT PRIMARY KEY, name TEXT)")
	run(t, "CREATE TABLE emps (id INT PRIMARY KEY, name TEXT, d_id INT, salary FLOAT, age INT)")

	// 2. Insert data
	run(t, "INSERT INTO depts (id, name) VALUES (1, 'Engineering')")
	run(t, "INSERT INTO depts (id, name) VALUES (2, 'Sales')")
	run(t, "INSERT INTO depts (id, name) VALUES (3, 'Marketing')")

	run(t, "INSERT INTO emps (id, name, d_id, salary, age) VALUES (1, 'Alice', 1, 100000.0, 30)")
	run(t, "INSERT INTO emps (id, name, d_id, salary, age) VALUES (2, 'Bob', 1, 90000.0, 25)")
	run(t, "INSERT INTO emps (id, name, d_id, salary, age) VALUES (3, 'Charlie', 2, 80000.0, 35)")
	run(t, "INSERT INTO emps (id, name, d_id, salary, age) VALUES (4, 'David', 2, 85000.0, 40)")
	run(t, "INSERT INTO emps (id, name, d_id, salary, age) VALUES (5, 'Eve', 3, 70000.0, 22)")

	// 3. Test Cases
	tests := []struct {
		name      string
		query     string
		checkRows int
		verify    func(t *testing.T, res *executor.Result)
	}{
		{
			"SELECT with IN operator",
			"SELECT * FROM emps WHERE d_id IN (1, 2)",
			4,
			nil,
		},
		{
			"SELECT with LIKE operator (prefix)",
			"SELECT * FROM emps WHERE name LIKE 'A%'",
			1,
			nil,
		},
		{
			"SELECT with ORDER BY multiple columns",
			"SELECT * FROM emps ORDER BY d_id ASC, salary DESC",
			5,
			func(t *testing.T, res *executor.Result) {
				// Alice (1, 100k) then Bob (1, 90k)
				if res.Rows[0]["name"] != "Alice" || res.Rows[1]["name"] != "Bob" {
					t.Errorf("Order BY d_id ASC, salary DESC failed")
				}
			},
		},
		{
			"Aggregates: SUM, AVG, MIN, MAX",
			"SELECT SUM(salary), AVG(salary), MIN(age), MAX(age) FROM emps",
			1,
			func(t *testing.T, res *executor.Result) {
				t.Logf("Aggregate Result Rows[0]: %+v", res.Rows[0])
				// 100+90+80+85+70 = 425,000
				if res.Rows[0]["SUM(salary)"] != 425000.0 {
					t.Errorf("SUM failed: expected 425000.0, got %v", res.Rows[0]["SUM(salary)"])
				}
				if res.Rows[0]["AVG(salary)"] != 85000.0 {
					t.Errorf("AVG failed: expected 85000.0, got %v", res.Rows[0]["AVG(salary)"])
				}
				if res.Rows[0]["MIN(age)"] != 22 {
					t.Errorf("MIN failed: expected 22, got %v", res.Rows[0]["MIN(age)"])
				}
				if res.Rows[0]["MAX(age)"] != 40 {
					t.Errorf("MAX failed: expected 40, got %v", res.Rows[0]["MAX(age)"])
				}
			},
		},
		{
			"GROUP BY with Aggregates",
			"SELECT d_id, COUNT(*) FROM emps GROUP BY d_id ORDER BY d_id",
			3,
			func(t *testing.T, res *executor.Result) {
				// d_id 1: 2, 2: 2, 3: 1
				if res.Rows[0]["COUNT(*)"] != 2 || res.Rows[1]["COUNT(*)"] != 2 || res.Rows[2]["COUNT(*)"] != 1 {
					t.Errorf("GROUP BY counts failed: row 0: %v, row 1: %v, row 2: %v", res.Rows[0], res.Rows[1], res.Rows[2])
				}
			},
		},
		{
			"GROUP BY with HAVING",
			"SELECT d_id, COUNT(*) FROM emps GROUP BY d_id HAVING COUNT(*) > 1 ORDER BY d_id",
			2,
			func(t *testing.T, res *executor.Result) {
				if len(res.Rows) != 2 {
					t.Errorf("HAVING filter failed")
				}
			},
		},
		{
			"Multi-table JOIN with aliases",
			"SELECT e.name, d.name AS dept_name FROM emps AS e INNER JOIN depts AS d ON e.d_id = d.id WHERE d.name = 'Engineering'",
			2,
			func(t *testing.T, res *executor.Result) {
				for _, row := range res.Rows {
					if row["dept_name"] != "Engineering" {
						t.Errorf("JOIN result incorrect: %v", row)
					}
				}
			},
		},
		{
			"Mathematical Operations (Arithmetic)",
			"SELECT name, salary + 5000 AS new_salary FROM emps WHERE age + 5 > 40",
			1,
			func(t *testing.T, res *executor.Result) {
				// Only David (age 40) matches age + 5 > 40
				if res.Rows[0]["name"] != "David" {
					t.Errorf("Arithmetic WHERE failed: expected David, got %v", res.Rows[0]["name"])
				}
				// David's salary 85000 + 5000 = 90000
				if res.Rows[0]["new_salary"] != 90000.0 {
					t.Errorf("Arithmetic SELECT failed: expected 90000.0, got %v", res.Rows[0]["new_salary"])
				}
			},
		},
		{
			"CASE WHEN (Basic Parsing Verification)",
			"SELECT name, CASE WHEN age > 30 THEN 'Senior' ELSE 'Junior' END AS seniority FROM emps",
			5,
			func(t *testing.T, res *executor.Result) {
				// For now, CASE WHEN might return 'computed_column' if not fully implemented in executor
				// We just verify it doesn't crash and returns the rows.
				if res.Columns[1] != "seniority" {
					t.Errorf("CASE WHEN alias failed: expected 'seniority', got '%s'", res.Columns[1])
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := run(t, tt.query)
			if tt.checkRows >= 0 && len(res.Rows) != tt.checkRows {
				t.Errorf("Expected %d rows, got %d", tt.checkRows, len(res.Rows))
			}
			if tt.verify != nil {
				tt.verify(t, res)
			}
		})
	}
}
