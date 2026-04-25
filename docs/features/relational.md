# Relational Integrity

GhostSQL implements standard relational features to ensure data consistency.

## Primary Keys

Ensure each row is unique:

```sql
CREATE TABLE users (
    id INT PRIMARY KEY,
    name TEXT
);
```

## Foreign Keys

Link tables together and enforce referential integrity:

```sql
CREATE TABLE departments (
    id INT PRIMARY KEY,
    name TEXT
);

CREATE TABLE employees (
    id INT PRIMARY KEY,
    name TEXT,
    dept_id INT REFERENCES departments(id)
);
```

## Joins

GhostSQL supports all major JOIN types:

*   **INNER JOIN**: Only rows with matches in both tables.
*   **LEFT JOIN**: All rows from the left table, plus matches from the right.
*   **RIGHT JOIN**: All rows from the right table, plus matches from the left.
*   **FULL OUTER JOIN**: All rows from both tables, with NULLs where no match exists.
*   **CROSS JOIN**: Cartesian product of both tables.

### Join Example

```sql
SELECT e.name, d.name 
FROM employees e 
LEFT JOIN departments d ON e.dept_id = d.id;
```
