"""
GhostSQL RBAC Integration Test — psycopg2 client
Tests PostgreSQL-standard ownership, privilege enforcement, and GRANT/REVOKE.
Usage: python3 tests/integration/test_psycopg2.py
"""
import sys
import psycopg2

# ── Connection helpers ─────────────────────────────────────────────────────────

BASE = {"host": "localhost", "port": 5433, "database": "ghostsql"}

def connect_as(user: str, password: str) -> psycopg2.extensions.connection:
    conn = psycopg2.connect(**BASE, user=user, password=password)
    conn.autocommit = True  # Mirrors psql's single-statement mode; avoids implicit BEGIN
    return conn

def ghost() -> psycopg2.extensions.connection:
    return connect_as("ghost", "ghost")

def exampleuser() -> psycopg2.extensions.connection:
    return connect_as("exampleuser_py", "example")


# ── Test helpers ───────────────────────────────────────────────────────────────

pass_count = 0
fail_count = 0

def ok(msg: str):
    global pass_count
    print(f"PASS: {msg}")
    pass_count += 1

def fail(msg: str):
    global fail_count
    print(f"FAIL: {msg}")
    fail_count += 1

def expect_denied(conn: psycopg2.extensions.connection, sql: str, label: str):
    """Execute SQL and assert it raises a permission denied error."""
    cur = conn.cursor()
    try:
        cur.execute(sql)
        fail(f"{label} — expected permission denied, but query succeeded")
    except Exception as e:
        if "permission denied" in str(e).lower():
            ok(label)
        else:
            fail(f"{label} — unexpected error: {e}")
    finally:
        # Connection is broken after error; reconnect is done by the caller
        try:
            conn.close()
        except Exception:
            pass


# ── Main test ─────────────────────────────────────────────────────────────────

def run():
    print("============================================")
    print(" GhostSQL RBAC Integration Test (psycopg2)")
    print("============================================")

    conn = None
    try:
        # 1. ghost creates exampleuser_py and grants CONNECT + CREATE
        print("\n--- 1. Setup: ghost creates exampleuser_py ---")
        conn = ghost()
        cur = conn.cursor()
        cur.execute("CREATE ROLE exampleuser_py WITH LOGIN PASSWORD 'example';")
        cur.execute("GRANT CONNECT ON DATABASE ghostsql TO exampleuser_py;")
        cur.execute("GRANT CREATE ON DATABASE ghostsql TO exampleuser_py;")
        conn.close()
        print("Role created and privileges granted.")

        # 2. ghost creates ghost_secrets_py (owned by ghost)
        print("\n--- 2. ghost creates ghost_secrets_py ---")
        conn = ghost()
        cur = conn.cursor()
        cur.execute("CREATE TABLE ghost_secrets_py (id INT, secret TEXT);")
        cur.execute("INSERT INTO ghost_secrets_py VALUES (1, 'Secret from Python');")
        conn.close()
        print("Table created and populated.")

        # 3. exampleuser reads ghost_secrets_py — should be denied (not owner, no grant)
        print("\n--- 3. exampleuser_py reads ghost_secrets_py (should be denied) ---")
        conn = exampleuser()
        expect_denied(
            conn,
            "SELECT * FROM ghost_secrets_py;",
            "Access correctly denied to non-owner without GRANT"
        )
        # expect_denied closes the connection; reconnect for step 4
        conn = None

        # 4. exampleuser creates user_data_py — succeeds as table owner
        print("\n--- 4. exampleuser_py creates user_data_py (owned by exampleuser_py) ---")
        conn = exampleuser()
        cur = conn.cursor()
        cur.execute("CREATE TABLE user_data_py (id INT, info TEXT);")
        cur.execute("INSERT INTO user_data_py VALUES (1, 'User data from Python');")
        conn.close()
        conn = None
        ok("exampleuser_py created and populated their own table")

        # 5. ghost reads user_data_py — succeeds as superuser
        print("\n--- 5. ghost reads user_data_py as superuser ---")
        conn = ghost()
        cur = conn.cursor()
        cur.execute("SELECT info FROM user_data_py WHERE id = 1;")
        row = cur.fetchone()
        conn.close()
        conn = None
        if row and "User data" in str(row[0]):
            ok(f"Superuser correctly bypasses ownership checks — got: {row}")
        else:
            fail(f"Ghost (superuser) could not read user-owned table — got: {row}")

        # 6. ghost grants SELECT on ghost_secrets_py to exampleuser_py
        print("\n--- 6. ghost GRANTs SELECT on ghost_secrets_py TO exampleuser_py ---")
        conn = ghost()
        cur = conn.cursor()
        cur.execute("GRANT SELECT ON TABLE ghost_secrets_py TO exampleuser_py;")
        conn.close()
        conn = None
        print("GRANT issued.")

        # 7. exampleuser reads ghost_secrets_py — succeeds (explicit GRANT)
        print("\n--- 7. exampleuser_py reads ghost_secrets_py after GRANT ---")
        conn = exampleuser()
        cur = conn.cursor()
        cur.execute("SELECT secret FROM ghost_secrets_py WHERE id = 1;")
        row = cur.fetchone()
        conn.close()
        conn = None
        if row and "Secret" in str(row[0]):
            ok(f"Access correctly granted after GRANT SELECT — got: {row}")
        else:
            fail(f"exampleuser_py could not read after GRANT SELECT — got: {row}")

        # 8. exampleuser attempts INSERT into ghost_secrets_py — denied (no INSERT grant)
        print("\n--- 8. exampleuser_py INSERTs into ghost_secrets_py (should be denied) ---")
        conn = exampleuser()
        expect_denied(
            conn,
            "INSERT INTO ghost_secrets_py VALUES (2, 'Hack attempt');",
            "INSERT correctly denied — only SELECT was granted"
        )
        conn = None

    finally:
        # Always clean up, even on failure
        print("\n--- Cleanup ---")
        try:
            conn = ghost()
            cur = conn.cursor()
            for stmt in [
                "DROP TABLE IF EXISTS ghost_secrets_py;",
                "DROP TABLE IF EXISTS user_data_py;",
                "DROP ROLE IF EXISTS exampleuser_py;",
            ]:
                try:
                    cur.execute(stmt)
                except Exception:
                    pass
            conn.close()
            print("Cleanup done.")
        except Exception as e:
            print(f"Cleanup error (non-fatal): {e}")

    print("\n============================================")
    print(f" Results: {pass_count} passed, {fail_count} failed")
    print("============================================")
    sys.exit(0 if fail_count == 0 else 1)


if __name__ == "__main__":
    run()
