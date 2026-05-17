package storage

import (
	"fmt"
	"sync"
)

// Cursor represents a declared SQL cursor
type Cursor struct {
	Name       string
	Query      interface{} // AST Select statement
	Rows       []Row
	CurrentIdx int
}

// Session represents a single client connection state
type Session struct {
	ID              string
	CurrentDatabase string
	User            string
	SessionUser     string // Initially authenticated user
	Variables        map[string]string
	TxLocalVariables map[string]string            // Original variable values saved before SET LOCAL
	TxActive         bool
	TxTables         map[string]*Table            // Table copies modified during transaction
	TxSavepoints     map[string]map[string]*Table // Table copies cloned at savepoints
	Cursors          map[string]*Cursor
	mu               sync.RWMutex
}

var DefaultSessionVariables = map[string]string{
	"search_path": "public",
	"work_mem":    "4MB",
	"timezone":    "UTC",
}

// NewSession creates a new client session
func NewSession(id string) *Session {
	vars := make(map[string]string)
	for k, v := range DefaultSessionVariables {
		vars[k] = v
	}

	return &Session{
		ID:               id,
		Variables:        vars,
		TxLocalVariables: make(map[string]string),
		TxTables:         make(map[string]*Table),
		TxSavepoints:     make(map[string]map[string]*Table),
		Cursors:          make(map[string]*Cursor),
	}
}

// SetDatabase sets the current database for this session
func (s *Session) SetDatabase(dbName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.CurrentDatabase = dbName
}

// GetDatabase gets the current database for this session
func (s *Session) GetDatabase() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.CurrentDatabase
}

// SetUser sets the current user for this session
func (s *Session) SetUser(user string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.User = user
}

// SetSessionUser sets the authenticated session user
func (s *Session) SetSessionUser(user string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.SessionUser = user
}

// GetUser gets the current user for this session
func (s *Session) GetUser() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.User == "" {
		return "anonymous"
	}
	return s.User
}

// SessionManager manages all active client sessions
type SessionManager struct {
	sessions map[string]*Session
	mu       sync.RWMutex
}

// NewSessionManager creates a new session manager
func NewSessionManager() *SessionManager {
	return &SessionManager{
		sessions: make(map[string]*Session),
	}
}

// CreateSession creates and registers a new session
func (sm *SessionManager) CreateSession(id string) *Session {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	
	session := NewSession(id)
	sm.sessions[id] = session
	return session
}

// GetSession retrieves a session by ID
func (sm *SessionManager) GetSession(id string) (*Session, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	
	session, exists := sm.sessions[id]
	if !exists {
		return nil, fmt.Errorf("session %s not found", id)
	}
	return session, nil
}

// CloseSession removes a session
func (sm *SessionManager) CloseSession(id string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	delete(sm.sessions, id)
}

// GetVariable retrieves a session variable
func (s *Session) GetVariable(name string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Variables[name]
}

// SetVariable sets a session variable
func (s *Session) SetVariable(name, val string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Variables[name] = val
}

// SetLocalVariable sets a transaction-scoped local variable.
// It stores the original value in s.TxLocalVariables if not already present,
// so that it can be restored on COMMIT or ROLLBACK.
func (s *Session) SetLocalVariable(name, val string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.TxActive {
		// SET LOCAL acts as normal SET if not in a transaction
		s.Variables[name] = val
		return
	}
	if _, saved := s.TxLocalVariables[name]; !saved {
		s.TxLocalVariables[name] = s.Variables[name]
	}
	s.Variables[name] = val
}

// ResetVariable restores a variable to its default value
func (s *Session) ResetVariable(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if defVal, exists := DefaultSessionVariables[name]; exists {
		s.Variables[name] = defVal
	} else {
		delete(s.Variables, name)
	}
}

