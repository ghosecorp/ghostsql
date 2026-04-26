# Security & Authentication

GhostSQL implements a robust authentication and authorization layer compatible with the standard PostgreSQL wire protocol.

## Authentication Overview

GhostSQL uses a **Host-Based Authentication (HBA)** system, controlled by `pg_hba.conf` in the data directory.

### Authentication Modes
- **Trust**: Permits connections without a password. Default for local loopback (127.0.0.1) connections for non-superusers.
- **Password**: Enforces a cleartext password challenge. Required for the administrative account (`ghost`) and remote connections.

## Default Credentials

| Parameter | Value |
| :--- | :--- |
| **Username** | `ghost` |
| **Password** | `ghost` |

## Role-Based Access Control (RBAC)

GhostSQL uses roles to manage database access and privileges.

### Role Types
- **Login Roles (Users)**: Created with the `LOGIN` attribute.
- **Group Roles**: Roles that act as groups to manage permissions for multiple users.
- **Superusers**: Administrative roles (like `ghost`) that bypass all permission checks.

### Table Ownership

When a user creates a table, they become its **owner** automatically — matching PostgreSQL's `pg_aclcheck` behavior. The owner always has full access without needing explicit `GRANT` entries. This means:

- `REVOKE` from the owner has no effect (the owner always wins)
- Ownership is stored in the table's `Owner` field (analogous to `pg_class.relowner`)
- Superusers bypass ownership and ACL checks regardless

### Example: Managing Roles
```sql
-- Create a user with a password
CREATE ROLE alice WITH LOGIN PASSWORD 'secret';

-- Grant connection and creation privileges (PostgreSQL standard order)
GRANT CONNECT ON DATABASE my_db TO alice;
GRANT CREATE ON DATABASE my_db TO alice;

-- Grant object-level privileges
GRANT SELECT ON TABLE users TO alice;

-- Revoke a privilege
REVOKE UPDATE ON TABLE orders FROM bob;

-- Drop a role
DROP ROLE alice;
```

### Supported Privileges

| Privilege | Object Type | Description |
|:---|:---|:---|
| `SELECT` | `TABLE` | Read rows |
| `INSERT` | `TABLE` | Insert rows |
| `UPDATE` | `TABLE` | Update rows |
| `DELETE` | `TABLE` | Delete rows |
| `CREATE` | `DATABASE` | Create tables in a database |
| `CONNECT` | `DATABASE` | Connect to a database |
| `ALL PRIVILEGES` | `TABLE`, `DATABASE` | All of the above |

## Row-Level Security (RLS)

Row-Level Security allows you to define policies that restrict which rows a user can see or modify.

### How it Works
1. **Enable RLS**: `ALTER TABLE my_table ENABLE ROW LEVEL SECURITY;`
2. **Define Policies**: Use `CREATE POLICY` to define the filtering logic.

### Example: Private Data
```sql
-- Users can only see rows where they are the owner
CREATE POLICY own_records ON documents
FOR SELECT
TO all
USING (owner = current_user());
```

## Driver Compatibility

GhostSQL handles the initialization queries that standard PostgreSQL drivers send automatically:

| Statement | Behavior |
|:---|:---|
| `SET datestyle TO 'ISO'` | Accepted as no-op |
| `BEGIN` | Accepted (no-op — transactions not yet ACID) |
| `COMMIT` | Accepted as no-op |
| `ROLLBACK` | Accepted as no-op |

This ensures compatibility with:
- **`psycopg2`** (Python)
- **`pgx`** (Go)
- **`psql`** (CLI)
- Any standard PostgreSQL wire-protocol client

## Connection Examples

### Using psql (Command Line)
```bash
# Trusted (local — no password required)
psql -h localhost -p 5433 -d ghostsql

# Authenticated (as ghost)
PGPASSWORD=ghost psql -h localhost -p 5433 -U ghost -d ghostsql
```

### Using psycopg2 (Python)
```python
import psycopg2
conn = psycopg2.connect(
    host="localhost", port=5433,
    database="ghostsql", user="ghost", password="ghost"
)
conn.autocommit = True
```

## Security Files
- **`pg_hba.conf`**: Connection rules (IP, database, user, method).
- **`global/pg_authid`**: Binary role store (analogous to PostgreSQL's `pg_authid` catalog).
