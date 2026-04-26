# SQL Reference

GhostSQL supports a broad subset of PostgreSQL-compatible SQL.

## Data Manipulation (DML)

### INSERT
```sql
INSERT INTO table_name (col1, col2) VALUES (val1, val2);
```
Response tag: `INSERT 0 n` (PostgreSQL standard, where `n` is the row count).

### UPDATE
```sql
UPDATE table_name SET col1 = val1 WHERE condition;
```

### DELETE
```sql
DELETE FROM table_name WHERE condition;
```

### TRUNCATE
```sql
TRUNCATE TABLE table_name;
```

## Data Definition (DDL)

### CREATE TABLE
```sql
CREATE TABLE table_name (
    column1 data_type [constraints],
    ...
);
```
The executing role becomes the **owner** of the table and has full access automatically.

### ALTER TABLE
```sql
ALTER TABLE table_name ADD COLUMN column_name data_type;
ALTER TABLE table_name ENABLE ROW LEVEL SECURITY;
```

### DROP TABLE
```sql
DROP TABLE table_name;
```

## Access Control (DCL)

### CREATE ROLE
```sql
CREATE ROLE rolename WITH LOGIN PASSWORD 'pass';
CREATE ROLE rolename WITH LOGIN PASSWORD 'pass' SUPERUSER;
```

### DROP ROLE
```sql
DROP ROLE rolename;
```

### GRANT
```sql
-- Database-level
GRANT CONNECT ON DATABASE dbname TO rolename;
GRANT CREATE ON DATABASE dbname TO rolename;

-- Table-level
GRANT SELECT ON TABLE tablename TO rolename;
GRANT INSERT, UPDATE, DELETE ON TABLE tablename TO rolename;
GRANT ALL PRIVILEGES ON TABLE tablename TO rolename;
```

### REVOKE
```sql
REVOKE SELECT ON TABLE tablename FROM rolename;
```

### CREATE POLICY (Row-Level Security)
```sql
CREATE POLICY policy_name ON table_name
FOR SELECT
TO all
USING (owner_column = current_user());
```

## Transaction Control

GhostSQL accepts but does not yet fully implement ACID transactions. These statements are parsed and accepted as no-ops:

```sql
BEGIN;
COMMIT;
ROLLBACK;
```

## Session Variables

```sql
SET datestyle TO 'ISO';   -- accepted as no-op (driver compatibility)
```

## Metadata & Discovery

### SHOW TABLES
List all tables in the current database.

### SHOW COLUMNS
```sql
SHOW COLUMNS FROM table_name;
```

### COMMENT ON
```sql
COMMENT ON TABLE table_name IS 'description';
```

### System Catalog (pg_catalog)
```sql
SELECT relname FROM pg_catalog.pg_class WHERE relkind = 'r';
SELECT nspname FROM pg_catalog.pg_namespace;
```
