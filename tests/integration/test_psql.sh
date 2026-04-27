#!/bin/bash
# GhostSQL RBAC Integration Test — psql client
# Usage: ./tests/integration/test_psql.sh

set -euo pipefail

HOST=localhost
PORT=5433
DB=ghostsql

ghost()       { PGPASSWORD=ghost    psql -h "$HOST" -p "$PORT" -U ghost           -d "$DB" -v ON_ERROR_STOP=1 "$@"; }
exampleuser() { PGPASSWORD=example  psql -h "$HOST" -p "$PORT" -U exampleuser_psql -d "$DB" -v ON_ERROR_STOP=1 "$@"; }

cleanup() {
    echo ""
    echo "--- Cleanup ---"
    PGPASSWORD=ghost psql -h "$HOST" -p "$PORT" -U ghost -d "$DB" \
        -c "DROP TABLE IF EXISTS ghost_secrets_psql;" \
        -c "DROP TABLE IF EXISTS user_data_psql;" \
        -c "DROP ROLE IF EXISTS exampleuser_psql;" 2>/dev/null || true
    echo "--- Cleanup done ---"
}
trap cleanup EXIT

PASS_COUNT=0
FAIL_COUNT=0
pass() { echo "PASS: $1"; ((PASS_COUNT++)); }
fail() { echo "FAIL: $1"; ((FAIL_COUNT++)); }

# capture_err: run psql without ON_ERROR_STOP so we can inspect the error message
capture_err() { PGPASSWORD=example psql -h "$HOST" -p "$PORT" -U exampleuser_psql -d "$DB" "$@" 2>&1 || true; }

echo "============================================"
echo " GhostSQL RBAC Integration Test (psql)"
echo "============================================"

echo ""
echo "--- 1. ghost creates exampleuser_psql ---"
ghost -c "CREATE ROLE exampleuser_psql WITH LOGIN PASSWORD 'example';"
ghost -c "GRANT CONNECT ON DATABASE ghostsql TO exampleuser_psql;"
ghost -c "GRANT CREATE ON DATABASE ghostsql TO exampleuser_psql;"
echo "Role created and privileges granted."

echo ""
echo "--- 2. ghost creates ghost_secrets_psql (owned by ghost) ---"
ghost -c "CREATE TABLE ghost_secrets_psql (id INT, secret TEXT);"
ghost -c "INSERT INTO ghost_secrets_psql VALUES (1, 'The superuser password is ghost');"
echo "Table created and populated."

echo ""
echo "--- 3. exampleuser_psql reads ghost_secrets_psql (should be denied) ---"
OUT=$(capture_err -c "SELECT * FROM ghost_secrets_psql;")
if echo "$OUT" | grep -q "permission denied"; then
    pass "Access correctly denied to non-owner without GRANT"
else
    fail "Expected permission denied, but access was allowed"
fi

echo ""
echo "--- 4. exampleuser_psql creates user_data_psql (owned by exampleuser_psql) ---"
exampleuser -c "CREATE TABLE user_data_psql (id INT, info TEXT);"
exampleuser -c "INSERT INTO user_data_psql VALUES (1, 'This is my private info');"
pass "exampleuser_psql created and populated their own table"

echo ""
echo "--- 5. ghost reads user_data_psql as superuser ---"
RESULT=$(ghost -t -c "SELECT info FROM user_data_psql WHERE id = 1;")
if echo "$RESULT" | grep -q "private info"; then
    pass "Superuser correctly bypasses ownership checks"
else
    fail "Ghost (superuser) could not read user-owned table"
fi

echo ""
echo "--- 6. ghost GRANTs SELECT on ghost_secrets_psql TO exampleuser_psql ---"
ghost -c "GRANT SELECT ON TABLE ghost_secrets_psql TO exampleuser_psql;"
echo "GRANT issued."

echo ""
echo "--- 7. exampleuser_psql reads ghost_secrets_psql after GRANT ---"
RESULT=$(exampleuser -t -c "SELECT secret FROM ghost_secrets_psql WHERE id = 1;")
if echo "$RESULT" | grep -q "superuser password"; then
    pass "exampleuser_psql correctly reads after explicit GRANT SELECT"
else
    fail "exampleuser_psql could not read after GRANT SELECT"
fi

echo ""
echo "--- 8. exampleuser_psql INSERTs into ghost_secrets_psql (should be denied) ---"
OUT=$(capture_err -c "INSERT INTO ghost_secrets_psql VALUES (2, 'Hack attempt');")
if echo "$OUT" | grep -q "permission denied"; then
    pass "INSERT correctly denied — only SELECT was granted"
else
    fail "Expected INSERT to be denied, but it succeeded"
fi

echo ""
echo "============================================"
echo " Results: $PASS_COUNT passed, $FAIL_COUNT failed"
echo "============================================"
[ "$FAIL_COUNT" -eq 0 ] && exit 0 || exit 1
