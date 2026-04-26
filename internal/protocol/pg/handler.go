package pg

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"

	"github.com/ghosecorp/ghostsql/internal/executor"
	"github.com/ghosecorp/ghostsql/internal/parser"
	"github.com/ghosecorp/ghostsql/internal/storage"
)

// Handler handles a single PostgreSQL connection
type Handler struct {
	conn     net.Conn
	db       *storage.Database
	session  *storage.Session
	executor *executor.Executor
	user     string
	role     *storage.Role
}

// NewHandler creates a new PG protocol handler
func NewHandler(conn net.Conn, db *storage.Database, session *storage.Session) *Handler {
	return &Handler{
		conn:     conn,
		db:       db,
		session:  session,
		executor: executor.NewExecutor(db, session),
	}
}

// Handle processes the connection
func (h *Handler) Handle() error {
	// 1. Initial Handshake (Startup/SSL)
	if err := h.handleStartup(); err != nil {
		return err
	}

	// 2. HBA Check
	var remoteIP net.IP
	if tcpAddr, ok := h.conn.RemoteAddr().(*net.TCPAddr); ok {
		remoteIP = tcpAddr.IP
	} else {
		remoteIP = net.ParseIP("127.0.0.1") // Fallback for pipes/local
	}

	hba, _ := LoadHBAConfig("pg_hba.conf") // Reload for now or cache?
	method, err := hba.Check(remoteIP, h.session.GetDatabase(), h.user)
	if err != nil {
		h.sendError(err)
		return err
	}

	if method == MethodReject {
		err := fmt.Errorf("connection rejected by pg_hba.conf")
		h.sendError(err)
		return err
	}

	// 3. Authentication
	role, exists := h.db.RoleStore.GetRole(h.user)
	if !exists {
		// If user doesn't exist, we can either reject or trust.
		// PostgreSQL by default rejects. We'll trust if not 'ghost' for now (per user req)
		if h.user == "ghost" {
			h.sendError(fmt.Errorf("role 'ghost' not found in system catalog"))
			return fmt.Errorf("ghost role missing")
		}
		
		// If HBA says trust, we trust. If it says password, we might need a password even for non-existent?
		// Actually if HBA says password, we MUST have a role to check against.
		if method == MethodTrust {
			if err := h.sendAuthenticationOk(); err != nil {
				return err
			}
		} else {
			h.sendError(fmt.Errorf("role %s does not exist and trust is not enabled", h.user))
			return fmt.Errorf("role not found")
		}
	} else if role.CanLogin {
		if method == MethodTrust {
			if err := h.sendAuthenticationOk(); err != nil {
				return err
			}
		} else {
			if err := h.requestPassword(role); err != nil {
				h.sendError(err)
				return err
			}
		}
		h.role = role
		h.session.SetUser(h.user)
	} else {
		// Role cannot login
		err := fmt.Errorf("role %s is not permitted to log in", h.user)
		h.sendError(err)
		return err
	}

	// 3. Send Parameter Status & ReadyForQuery
	if err := h.sendParameterStatus("server_version", "0.1.0"); err != nil {
		return err
	}
	if err := h.sendParameterStatus("client_encoding", "UTF8"); err != nil {
		return err
	}
	if err := h.sendParameterStatus("standard_conforming_strings", "on"); err != nil {
		return err
	}

	if err := h.sendReadyForQuery(); err != nil {
		return err
	}

	// 4. Main Loop
	for {
		msgType, payload, err := h.readMessage()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		switch msgType {
		case MsgQuery:
			if err := h.handleQuery(payload); err != nil {
				h.sendError(err)
			}
		case MsgTerminate:
			return nil
		default:
			h.db.Logger.Info("Unsupported message type: %c", msgType)
		}

		if err := h.sendReadyForQuery(); err != nil {
			return err
		}
	}
}

func (h *Handler) readMessage() (byte, []byte, error) {
	header := make([]byte, 5)
	if _, err := io.ReadFull(h.conn, header); err != nil {
		return 0, nil, err
	}

	msgType := header[0]
	length := binary.BigEndian.Uint32(header[1:]) - 4

	payload := make([]byte, length)
	if _, err := io.ReadFull(h.conn, payload); err != nil {
		return 0, nil, err
	}

	return msgType, payload, nil
}

func (h *Handler) handleStartup() error {
	// First 4 bytes are length
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(h.conn, lenBuf); err != nil {
		return err
	}
	length := binary.BigEndian.Uint32(lenBuf) - 4

	payload := make([]byte, length)
	if _, err := io.ReadFull(h.conn, payload); err != nil {
		return err
	}

	protocol := binary.BigEndian.Uint32(payload[:4])
	if protocol == 80877103 { // SSLRequest
		// We don't support SSL yet, send 'N'
		h.conn.Write([]byte{'N'})
		return h.handleStartup() // Read the real StartupMessage
	}

	// Parse StartupMessage (key-value pairs)
	params := make(map[string]string)
	data := payload[4:]
	for len(data) > 0 {
		key := h.readString(&data)
		if key == "" {
			break
		}
		val := h.readString(&data)
		params[key] = val
	}

	if dbName, ok := params["database"]; ok {
		h.session.SetDatabase(dbName)
	}

	if user, ok := params["user"]; ok {
		h.user = user
	}

	return nil
}

func (h *Handler) readString(data *[]byte) string {
	for i, b := range *data {
		if b == 0 {
			s := string((*data)[:i])
			*data = (*data)[i+1:]
			return s
		}
	}
	return ""
}

func (h *Handler) sendAuthenticationOk() error {
	msg := make([]byte, 9)
	msg[0] = ResAuthentication
	binary.BigEndian.PutUint32(msg[1:5], 8)
	binary.BigEndian.PutUint32(msg[5:9], 0) // Auth OK
	_, err := h.conn.Write(msg)
	return err
}

func (h *Handler) requestPassword(role *storage.Role) error {
	// Send AuthenticationCleartextPassword (3)
	msg := make([]byte, 9)
	msg[0] = ResAuthentication
	binary.BigEndian.PutUint32(msg[1:5], 8)
	binary.BigEndian.PutUint32(msg[5:9], 3) 
	if _, err := h.conn.Write(msg); err != nil {
		return err
	}

	// Read PasswordMessage
	msgType, payload, err := h.readMessage()
	if err != nil {
		return err
	}

	if msgType != MsgPassword {
		return fmt.Errorf("expected password message, got %c", msgType)
	}

	password := string(payload[:len(payload)-1]) // Remove null terminator
	if !role.VerifyPassword(password) {
		return fmt.Errorf("invalid password for user %s", h.user)
	}

	return h.sendAuthenticationOk()
}

func (h *Handler) sendParameterStatus(name, value string) error {
	buf := make([]byte, 0)
	buf = append(buf, ResParameterStatus)
	
	lenPos := len(buf)
	buf = append(buf, 0, 0, 0, 0)
	
	buf = append(buf, name...)
	buf = append(buf, 0)
	buf = append(buf, value...)
	buf = append(buf, 0)

	binary.BigEndian.PutUint32(buf[lenPos:], uint32(len(buf)-lenPos))
	_, err := h.conn.Write(buf)
	return err
}

func (h *Handler) sendReadyForQuery() error {
	msg := []byte{ResReadyForQuery, 0, 0, 0, 5, 'I'} // 'I' for Idle
	_, err := h.conn.Write(msg)
	return err
}

func (h *Handler) handleQuery(payload []byte) error {
	query := string(payload[:len(payload)-1]) // Remove null terminator
	h.db.Logger.Info("Executing query: %s", query)

	p := parser.NewParser(query)
	stmt, err := p.Parse()
	if err != nil {
		return err
	}

	result, err := h.executor.Execute(stmt)
	if err != nil {
		return err
	}

	// 1. Send RowDescription if it's a SELECT
	if len(result.Columns) > 0 {
		if err := h.sendRowDescription(result.Columns); err != nil {
			return err
		}

		// 2. Send DataRows
		for _, row := range result.Rows {
			if err := h.sendDataRow(result.Columns, row); err != nil {
				return err
			}
		}
	}

	// 3. Send CommandComplete
	return h.sendCommandComplete(result.Message)
}

func (h *Handler) sendRowDescription(columns []string) error {
	// Placeholder implementation - will map types later
	buf := make([]byte, 0)
	buf = append(buf, ResRowDescription)
	
	// Pre-calculate length
	lenPos := len(buf)
	buf = append(buf, 0, 0, 0, 0)

	// Field count
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(columns)))

	for _, col := range columns {
		buf = append(buf, col...)
		buf = append(buf, 0) // Null terminator
		buf = binary.BigEndian.AppendUint32(buf, 0) // Table OID
		buf = binary.BigEndian.AppendUint16(buf, 0) // Column index
		buf = binary.BigEndian.AppendUint32(buf, OIDText) // Type OID (default to text)
		buf = binary.BigEndian.AppendUint16(buf, 65535) // Type size
		buf = binary.BigEndian.AppendUint32(buf, 0) // Typmod
		buf = binary.BigEndian.AppendUint16(buf, 0) // Format code (0 = text)
	}

	// Set length
	binary.BigEndian.PutUint32(buf[lenPos:], uint32(len(buf)-lenPos))
	_, err := h.conn.Write(buf)
	return err
}

func (h *Handler) sendDataRow(columns []string, row storage.Row) error {
	buf := make([]byte, 0)
	buf = append(buf, ResDataRow)
	
	lenPos := len(buf)
	buf = append(buf, 0, 0, 0, 0)

	buf = binary.BigEndian.AppendUint16(buf, uint16(len(columns)))

	for _, col := range columns {
		val := row[col]
		if val == nil {
			buf = binary.BigEndian.AppendUint32(buf, 0xFFFFFFFF) // Null
		} else {
			strVal := fmt.Sprintf("%v", val)
			buf = binary.BigEndian.AppendUint32(buf, uint32(len(strVal)))
			buf = append(buf, strVal...)
		}
	}

	binary.BigEndian.PutUint32(buf[lenPos:], uint32(len(buf)-lenPos))
	_, err := h.conn.Write(buf)
	return err
}

func (h *Handler) sendCommandComplete(tag string) error {
	if tag == "" {
		tag = "SELECT"
	}
	
	buf := make([]byte, 0)
	buf = append(buf, ResCommandComplete)
	
	lenPos := len(buf)
	buf = append(buf, 0, 0, 0, 0)
	
	buf = append(buf, tag...)
	buf = append(buf, 0)

	binary.BigEndian.PutUint32(buf[lenPos:], uint32(len(buf)-lenPos))
	_, err := h.conn.Write(buf)
	return err
}

func (h *Handler) sendError(err error) {
	// Simplified ErrorResponse
	buf := make([]byte, 0)
	buf = append(buf, ResErrorResponse)
	
	lenPos := len(buf)
	buf = append(buf, 0, 0, 0, 0)
	
	buf = append(buf, 'S') // Severity
	buf = append(buf, "ERROR"...)
	buf = append(buf, 0)
	
	buf = append(buf, 'M') // Message
	buf = append(buf, err.Error()...)
	buf = append(buf, 0)
	
	buf = append(buf, 0) // Terminator

	binary.BigEndian.PutUint32(buf[lenPos:], uint32(len(buf)-lenPos))
	h.conn.Write(buf)
}
