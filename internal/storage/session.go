package storage

import (
	"fmt"
	"sync"
)

// Session represents a single client connection state
type Session struct {
	ID              string
	CurrentDatabase string
	User            string
	mu              sync.RWMutex
}

// NewSession creates a new client session
func NewSession(id string) *Session {
	return &Session{
		ID: id,
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
