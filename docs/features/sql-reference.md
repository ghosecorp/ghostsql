# SQL Reference

GhostSQL supports a broad subset of PostgreSQL-compatible SQL.

## Data Manipulation (DML)

### INSERT
```sql
INSERT INTO table_name (col1, col2) VALUES (val1, val2);
```

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

### ALTER TABLE
```sql
ALTER TABLE table_name ADD COLUMN column_name data_type;
```

### DROP TABLE
```sql
DROP TABLE table_name;
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
