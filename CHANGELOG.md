# Changelog

All notable changes to GhostSQL will be documented in this file.

## [0.1.2] - 2026-04-26

### Added
- **PostgreSQL RBAC (Role-Based Access Control)**: Implemented a robust role and privilege system.
  - Support for `CREATE ROLE` with `LOGIN`, `SUPERUSER`, and `PASSWORD` attributes.
  - Expanded `GRANT/REVOKE` support for `TABLE`, `DATABASE`, and `SCHEMA` objects.
  - Cluster-wide role persistence in `global/pg_auth.json`.
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
