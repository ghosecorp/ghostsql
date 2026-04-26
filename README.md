***

# GhostSQL

**GhostSQL** is a high-performance, PostgreSQL-compatible SQL database by **Ghosecorp**, designed for modern applications that need scalable relational data _and_ fast vector search for AI/ML workloads.

## Features

- **PostgreSQL-like SQL syntax**: Familiar for developers
- **Vector support**: Store and search embeddings with `VECTOR` type
- **HNSW Indexing**: Fast approximate nearest neighbor via `CREATE INDEX ... USING HNSW`
- **Relational integrity**: JOIN (INNER, LEFT, RIGHT, FULL OUTER, CROSS), FOREIGN KEY, PRIMARY KEY, NOT NULL
- **Data types**: INT, BIGINT, TEXT, VARCHAR(n), VECTOR(n), FLOAT, BOOLEAN
- **Aggregates**: COUNT, SUM, AVG, MIN, MAX with GROUP BY/HAVING
- **Advanced Security**: 
  - **RBAC**: PostgreSQL-compatible roles, privileges, and `GRANT/REVOKE`
  - **RLS**: Row-Level Security with `CREATE POLICY` and `current_user()` session filtering
  - **HBA**: IP-based access control via `pg_hba.conf`
- **Other SQL**: WHERE, ORDER BY, LIMIT, OFFSET, LIKE
- **Transaction-safe storage**: Binary format, slotted pages, persistence to disk

## Getting Started

### Build & Run

```bash
make build
./ghostsql-server
```

### Docker (Recommended)

Run GhostSQL in a containerized environment:

```bash
docker-compose up -d
```

This will start the server on port `5433` and persist data in a Docker volume.

## RBAC & Row-Level Security

GhostSQL implements robust PostgreSQL-style access control.

### 1. Role-Based Access Control (RBAC)

Create roles and manage privileges:

```sql
-- Create a new user
CREATE ROLE alice WITH LOGIN PASSWORD 'secret_pass';

-- Create a group role (no login)
CREATE ROLE analysts;

-- Grant privileges
GRANT SELECT ON TABLE sensitive_data TO alice;
GRANT ALL PRIVILEGES ON DATABASE my_db TO analysts;

-- Revoke privileges
REVOKE INSERT ON TABLE logs FROM alice;
```

### 2. Row-Level Security (RLS)

Filter rows dynamically based on the session user:

```sql
-- Enable RLS on a table
ALTER TABLE secrets ENABLE ROW LEVEL SECURITY;

-- Create a policy allowing users to see only their own data
CREATE POLICY own_secrets ON secrets 
FOR SELECT 
TO all 
USING (owner = current_user());
```

### 3. IP Access Control (HBA)

GhostSQL uses `pg_hba.conf` (located in the data directory) to manage connection rules. Default rules allow local `trust` but require `password` for the `ghost` superuser.

## Example Database Creation

```sql
-- Create tables for employees and departments
CREATE TABLE departments (
    id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL
);

CREATE TABLE employees (
    id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    dept_id INT REFERENCES departments(id)
);

-- Insert data
INSERT INTO departments VALUES (1, 'Engineering');
INSERT INTO departments VALUES (2, 'Sales');
INSERT INTO employees VALUES (1, 'Alice', 1);
INSERT INTO employees VALUES (2, 'Bob', 2);

-- Vector table example (pgvector style)
CREATE TABLE embeddings (
    id INT PRIMARY KEY,
    text TEXT,
    embedding VECTOR(4)
);

INSERT INTO embeddings VALUES (1, 'hello world', [0.1, 0.2, 0.3, 0.4]);
INSERT INTO embeddings VALUES (2, 'another', [0.2, 0.1, 0.3, 0.4]);
```

## JOINs and Relational Queries

```sql
-- Find employees and their department names
SELECT employees.name, departments.name
FROM employees
INNER JOIN departments ON employees.dept_id = departments.id;

-- Left join to get all employees, even without department
SELECT employees.name, departments.name
FROM employees
LEFT JOIN departments ON employees.dept_id = departments.id;

-- Right join to get all departments, even if no employees exist
SELECT employees.name, departments.name
FROM employees
RIGHT JOIN departments ON employees.dept_id = departments.id;
```

## Vector Search Example

```sql
-- Create vector index for fast similarity search
CREATE INDEX embeddings_idx ON embeddings USING HNSW (embedding) WITH (m=16, ef_construction=200);

-- Retrieve the two closest rows to a query vector (cosine similarity)
SELECT id, text
FROM embeddings
ORDER BY COSINE_DISTANCE(embedding, [0.1, 0.2, 0.3, 0.4])
LIMIT 2;
```

## Advanced SQL

```sql
-- Aggregates and grouping
SELECT dept_id, COUNT(*) AS num_employees
FROM employees
GROUP BY dept_id
HAVING COUNT(*) > 1;

-- Filtering
SELECT name FROM employees WHERE name LIKE '%Ali%';
```

## Security & Authentication

By default, the server operates in a secure-by-default mode:

- **Trusted Mode**: Local loopback connections (127.0.0.1) for non-superuser accounts are trusted.
- **Password Mode**: The administrative account (`ghost`) and remote connections always require password verification.

**Default Credentials:**
- **Username**: `ghost`
- **Password**: `ghost`

### Connecting via psql
```bash
# Trusted (skip password if connecting locally)
psql -h localhost -p 5433 -d ghostsql

# Authenticated
psql -h localhost -p 5433 -d ghostsql -U ghost -W
```

## Status

**Beta** — GhostSQL is suitable for prototyping, RAG setups, local semantic search, and scalable microservice data. Production features (index persistence, full ACID transactions) coming soon.

***

**License**: Apache 2.0

**Contributing**: Pull requests, GitHub issues, feature suggestions welcome!
