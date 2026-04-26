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

## Aggregates & Grouping

GhostSQL supports standard aggregate functions and grouping:

*   **COUNT(*) / COUNT(column)**
*   **SUM(column)**
*   **AVG(column)**
*   **MIN(column) / MAX(column)**

### Example: Salary Analysis
```sql
SELECT dept_id, AVG(salary), COUNT(*) 
FROM employees 
GROUP BY dept_id 
HAVING AVG(salary) > 50000;
```

## Mathematical Expressions

You can perform arithmetic operations directly in your queries:

```sql
-- Calculate bonus and total compensation
SELECT name, salary * 0.1 AS bonus, salary + (salary * 0.1) AS total 
FROM employees 
WHERE (salary + 5000) < 100000;
```
