# Testing

GhostSQL uses a robust testing framework to ensure stability and correctness.

## Integration Tests

The primary test suite is located in `tests/executor_test.go`. It performs end-to-end verification by simulating a full database session.

### Running Tests

To run the integration tests with detailed output:

```bash
go test -v ./tests/...
```

### What is Tested

*   **DML & DDL**: Table creation, insertion, updating, and deletion.
*   **Vector Similarity**: Verification of L2 and Cosine distance operators.
*   **Relational Logic**: Verification of all JOIN types and Foreign Key enforcement.
*   **Protocol**: Virtualized `pg_catalog` discovery for `psql` compatibility.

## Adding New Tests

When adding features, please add a corresponding test case to the `tests` slice in `executor_test.go`. Ensure you specify:
*   `name`: Descriptive name for the test.
*   `query`: The SQL string to execute.
*   `expected`: Expected success message (if any).
*   `expectedError`: Substring of the error message if the query should fail.
*   `checkRows`: Number of rows expected in the result set.
