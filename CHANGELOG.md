# Changelog

All notable changes to GhostSQL will be documented in this file.

## [0.1.1] - 2026-04-25

### Added
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
