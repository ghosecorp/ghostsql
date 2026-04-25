# Security & Authentication

GhostSQL implements a robust authentication layer compatible with the standard PostgreSQL wire protocol. This allows you to secure your data while maintaining compatibility with your favorite database tools and libraries.

## Authentication Overview

GhostSQL uses a **Dual-Mode Authentication** strategy to balance security with developer productivity.

### Trusted Mode (Default for Development)
In **Trusted Mode**, the server permits connections without a password if the provided username does *not* match the internal administrative account. 

- **Behavior**: Bypasses the password challenge.
- **Use Case**: Local development, CI/CD pipelines, and rapid prototyping.
- **Example**: `psql -h localhost -d ghostsql` (uses your current OS username).

### Password Mode
When connecting as the administrative user (`ghost`), the server enforces a **Cleartext Password Challenge**. 

- **Behavior**: Requests a password before granting access.
- **Use Case**: Shared environments, production-lite setups, and administrative tasks.
- **Example**: `psql -h localhost -U ghost -W`

## Default Credentials

| Parameter | Value |
| :--- | :--- |
| **Username** | `ghost` |
| **Password** | `ghostsql` |

> [!WARNING]
> While currently established as a specialized development initiative, future versions of GhostSQL will introduce Role-Based Access Control (RBAC) and IP-based filtering for enhanced security.

## Connection Examples

### Using psql (Command Line)

**Unauthenticated (Trusted):**
```bash
psql -h localhost -p 5433 -d my_app
```

**Authenticated (Ghost User):**
```bash
psql -h localhost -p 5433 -U ghost -d my_app -W
```

### Using Connection Strings

You can also connect using standard PostgreSQL URI strings:

**Trusted:**
`postgresql://localhost:5433/my_app`

**Authenticated:**
`postgresql://ghost:ghostsql@localhost:5433/my_app`

## Future Roadmap

The Ghosecorp team is planning several security enhancements:
1. **SCRAM-SHA-256 Support**: More secure password hashing for the wire protocol.
2. **RBAC**: Define granular permissions for different users and roles.
3. **TLS/SSL Encryption**: Secure data in transit.
4. **IP White-listing**: Restrict access to specific network ranges.
