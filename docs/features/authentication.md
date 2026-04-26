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

### Example: Managing Roles
```sql
-- Create a user with a password
CREATE ROLE alice WITH LOGIN PASSWORD 'secret';

-- Grant SELECT privilege on a table
GRANT SELECT ON TABLE users TO alice;

-- Revoke a privilege
REVOKE UPDATE ON TABLE orders FROM bob;
```

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

## Connection Examples

### Using psql (Command Line)
```bash
# Trusted (local)
psql -h localhost -p 5433 -d ghostsql

# Authenticated (as ghost)
psql -h localhost -p 5433 -U ghost -W
```

## Security Files
- **`pg_hba.conf`**: Connection rules (IP, database, user, method).
- **`global/pg_auth.json`**: Persisted role information and privileges.
