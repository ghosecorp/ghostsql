package tests

import (
	"encoding/binary"
	"io"
	"net"
	"os"
	"testing"

	"github.com/ghosecorp/ghostsql/internal/protocol/pg"
	"github.com/ghosecorp/ghostsql/internal/storage"
)

func TestPostgreSQLAuthentication(t *testing.T) {
	// Setup
	tmpDir := "./test_auth_dir"
	os.MkdirAll(tmpDir, 0755)
	defer os.RemoveAll(tmpDir)

	db, err := storage.Initialize(tmpDir)
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}

	t.Run("Anonymous Login (Trusted)", func(t *testing.T) {
		server, client := net.Pipe()
		defer server.Close()
		defer client.Close()

		session := storage.NewSession("test-anon")
		handler := pg.NewHandler(server, db, session)

		// Run handler in background
		go func() {
			defer server.Close()
			handler.Handle()
		}()

		// Simulate StartupMessage with no user
		sendStartupMessage(client, map[string]string{"database": "ghostsql"})

		// Expect AuthenticationOK ('R')
		resp := make([]byte, 9)
		io.ReadFull(client, resp)
		if resp[0] != 'R' || binary.BigEndian.Uint32(resp[5:9]) != 0 {
			t.Errorf("Expected AuthenticationOK (R, 0), got %c, %d", resp[0], binary.BigEndian.Uint32(resp[5:9]))
		}
	})

	t.Run("Ghost User Login (Requires Password)", func(t *testing.T) {
		server, client := net.Pipe()
		defer server.Close()
		defer client.Close()

		session := storage.NewSession("test-ghost")
		handler := pg.NewHandler(server, db, session)

		go func() {
			defer server.Close()
			handler.Handle()
		}()

		// Simulate StartupMessage with user='ghost'
		sendStartupMessage(client, map[string]string{"user": "ghost", "database": "ghostsql"})

		// Expect AuthenticationCleartextPassword (R, 3)
		resp := make([]byte, 9)
		io.ReadFull(client, resp)
		if resp[0] != 'R' || binary.BigEndian.Uint32(resp[5:9]) != 3 {
			t.Fatalf("Expected AuthenticationCleartextPassword (R, 3), got %c, %d", resp[0], binary.BigEndian.Uint32(resp[5:9]))
		}

		// Send PasswordMessage ('p')
		password := "ghost"
		payload := append([]byte(password), 0)
		header := make([]byte, 5)
		header[0] = 'p'
		binary.BigEndian.PutUint32(header[1:], uint32(len(payload)+4))
		client.Write(header)
		client.Write(payload)

		// Expect AuthenticationOK ('R', 0)
		io.ReadFull(client, resp)
		if resp[0] != 'R' || binary.BigEndian.Uint32(resp[5:9]) != 0 {
			t.Errorf("Expected AuthenticationOK after correct password, got %c, %d", resp[0], binary.BigEndian.Uint32(resp[5:9]))
		}
	})

	t.Run("Ghost User Login (Wrong Password)", func(t *testing.T) {
		server, client := net.Pipe()
		defer server.Close()
		defer client.Close()

		session := storage.NewSession("test-ghost-fail")
		handler := pg.NewHandler(server, db, session)

		go func() {
			defer server.Close()
			handler.Handle()
		}()

		sendStartupMessage(client, map[string]string{"user": "ghost"})

		// Expect AuthenticationCleartextPassword
		resp := make([]byte, 9)
		io.ReadFull(client, resp)

		// Send Wrong Password
		password := "wrong_pass"
		payload := append([]byte(password), 0)
		header := make([]byte, 5)
		header[0] = 'p'
		binary.BigEndian.PutUint32(header[1:], uint32(len(payload)+4))
		client.Write(header)
		client.Write(payload)

		// Expect ErrorResponse ('E')
		errResp := make([]byte, 1)
		io.ReadFull(client, errResp)
		if errResp[0] != 'E' {
			t.Errorf("Expected ErrorResponse (E), got %c", errResp[0])
		}
	})
}

func sendStartupMessage(conn net.Conn, params map[string]string) {
	buf := make([]byte, 0)
	buf = binary.BigEndian.AppendUint32(buf, 196608) // Protocol 3.0

	for k, v := range params {
		buf = append(buf, k...)
		buf = append(buf, 0)
		buf = append(buf, v...)
		buf = append(buf, 0)
	}
	buf = append(buf, 0)

	length := uint32(len(buf) + 4)
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, length)

	conn.Write(lenBuf)
	conn.Write(buf)
}
