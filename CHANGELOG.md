# Changelog

All notable changes to GhostSQL will be documented in this file.

## [0.1.3] - 2026-04-26

### Added
- **PostgreSQL Driver Compatibility (`SET`, `BEGIN`, `COMMIT`, `ROLLBACK`)**:
  - Implemented `SET <var> TO <value>` as a no-op accepted by the parser and executor.
  - Implemented `BEGIN`, `COMMIT`, and `ROLLBACK` as no-op `TransactionStmt` nodes, unblocking `psycopg2`, `pgx`, and other drivers that issue these commands automatically on connection.
- **PostgreSQL-Standard Table Ownership**:
  - Added `Owner` field to `Table` struct, set to the creating role at `CREATE TABLE` time.
  - `checkPrivilege` now performs an **owner bypass** for TABLE objects (analogous to `pg_aclcheck` in PostgreSQL): the table owner always has full access without requiring explicit `GRANT` entries.
  - Removed the non-standard approach of storing explicit `GRANT` entries for the creator.
- **`DROP ROLE` Support**:
  - Added `DropRoleStmt` AST node, `parseDropRole` in the parser, `DeleteRole()` on `RoleStore`, and `executeDropRole` in the executor.
  - Only superusers or roles with `CREATEROLE` can drop roles (matching PostgreSQL).
- **`GRANT`/`REVOKE CREATE` Support**:
  - Added `TOKEN_CREATE` to the recognized privilege token list in `parseGrant` and `parseRevoke`, enabling `GRANT CREATE ON DATABASE ... TO ...`.
- **`GRANT CONNECT` Support**:
  - Integration tests now issue `GRANT CONNECT ON DATABASE ghostsql TO <role>` before `GRANT CREATE`, following the correct PostgreSQL privilege hierarchy.
- **`DROP IF EXISTS` Cleanup Pattern**:
  - Integration test cleanup uses `DROP TABLE IF EXISTS` and `DROP ROLE IF EXISTS` to avoid errors when cleanup runs after a partial test failure.

### Fixed
- **`INSERT` Command Tag**: Changed `INSERT %d row(s)` to PostgreSQL-standard `INSERT 0 %d` format (OID field always 0 in modern PostgreSQL).
- **`GRANT CREATE` Parse Failure**: `GRANT CREATE ON DATABASE ...` was failing with "expected ON after privileges" because `CREATE` is lexed as `TOKEN_CREATE`, not `TOKEN_IDENT`. Fixed by adding `TOKEN_CREATE` to the privilege loop in both `parseGrant` and `parseRevoke`.
- **Superuser Privilege Check**: `checkPrivilege` now looks up the role's `IsSuperuser` flag in the `RoleStore` in addition to the legacy `ghost` name check, making it robust for any role created with `SUPERUSER`.

### Testing
- Updated `tests/executor_test.go` INSERT expectations to use the PostgreSQL-standard `INSERT 0 n` format.
- **Rewrote `tests/integration/test_psql.sh`**:
  - Added `_psql` suffix to all role/table names to avoid namespace collision with the Python test.
  - Added `capture_err` helper to fix a `set -euo pipefail` + `-v ON_ERROR_STOP=1` interaction that caused false "access allowed" failures in step 3.
  - Added `GRANT CONNECT` before `GRANT CREATE`.
  - Cleanup now uses `DROP IF EXISTS` and runs in a `trap EXIT` handler.
  - Added structured `PASS`/`FAIL` counting with a final summary and CI-friendly exit code.
- **Rewrote `tests/integration/test_psycopg2.py`**:
  - Added `_py` suffix to all role/table names.
  - All connections use `autocommit = True` to prevent implicit `BEGIN` from interfering with single-statement tests.
  - Added Step 8 (INSERT denial test) to match the psql test suite.
  - Cleanup moved to a `finally` block — always runs even on test failure.
  - Added structured pass/fail counting with CI-friendly exit code.

### Documentation
- Updated `CHANGELOG.md`, `README.md`, `docs/features/authentication.md`, and `docs/features/sql-reference.md`.

## [0.1.2] - 2026-04-26

### Added
- **PostgreSQL RBAC (Role-Based Access Control)**: Implemented a robust role and privilege system.
  - Support for `CREATE ROLE` with `LOGIN`, `SUPERUSER`, and `PASSWORD` attributes.
  - Expanded `GRANT/REVOKE` support for `TABLE`, `DATABASE`, and `SCHEMA` objects.
  - Cluster-wide role and privilege persistence.
- **Row-Level Security (RLS)**: Implemented a powerful security engine for granular data access.
  - Support for `CREATE POLICY` with `USING` expressions.
  - Runtime policy injection and session-aware filtering using `current_user()`.
  - Recursive logic merging for complex security policies.
- **HBA (Host-Based Authentication)**: Added `pg_hba.conf` support for IP-based access control.
  - Secure-by-default configuration (local trust for users, password for superuser).
- **Docker Integration**: Added `Dockerfile` and `docker-compose.yml` for containerized deployments.
- **Enhanced Parser**: Added support for `CURRENT_USER` keyword and function calls in expressions.
- **New Integration Tests**: Added dedicated suites for RBAC (`tests/rbac_test.go`) and RLS (`tests/rls_test.go`).

### Fixed
- **.gitignore Scoping**: Fixed root-level binary ignore rules that were accidentally hiding source directories.
- **Recursive Expression Safety**: Implemented `Clone()` for `WhereClause` to prevent cross-session security filter leakage.

### Documentation
- Updated `README.md` and `docs/features/authentication.md` with RBAC, RLS, and Docker instructions.

## [0.1.1] - 2026-04-25

### Added
- **PostgreSQL Authentication**: Implemented standard cleartext authentication flow with configurable credentials (default: `ghost`/`ghostsql`).
- **Flexible Connection Modes**: Added support for 'Trusted' connections (skip auth for local development) while enforcing 'Password' mode for specific users.
- **Improved Test Isolation**: Refactored `storage.Initialize` to support custom data paths, enabling concurrent test execution without lock file collisions.
- **Comprehensive Integration Test Suite**: Added `tests/executor_test.go` covering full database lifecycle (CRUD, DDL, DML).
- **PostgreSQL Vector Operators**: Added support for `<->` (L2 distance) and `<=>` (Cosine distance) in the lexer and parser.
- **Enhanced JOIN support**: Verified and tested INNER, LEFT, RIGHT, FULL OUTER, and CROSS joins.
- **Foreign Key Enforcement**: Added strict runtime verification of foreign key constraints during insertion.
- **Virtual Catalog Expansion**: Implemented `pg_attribute`, `pg_attrdef`, `pg_type`, `pg_collation`, and `pg_constraint` for better `psql` compatibility.
- **Result Set Reliability**: Implemented automatic unique column naming for JOINs (e.g., `name`, `name_1`) to prevent key collisions in result maps.

### Fixed
- **psql Compatibility**: Fixed `RowDescription` binary protocol to prevent segmentation faults in the official PostgreSQL client.
- **Parser Robustness**: Added strict "trailing junk" detection to prevent partial query execution.
- **Metadata Visibility**: Fixed table/column visibility in `psql` metadata commands like `\d`.

### Testing
- Added logging and progress tracking to integration tests.
- Support for expected error verification in test cases (e.g., verifying FK failure).

### Documentation
- **New Documentation Site**: Launched a modern, Material-themed documentation site powered by MkDocs.
- **Automated Deployment**: Configured GitHub Actions for automated documentation publishing on push.
- **Project Branding**: Integrated official Ghosecorp logos, team profiles, and professional contact information.
- **Expanded Guides**: Added detailed Architecture, Testing, and SQL Reference guides.
- **Community Focus**: Introduced a "How to Contribute" guide to encourage open-source participation.
