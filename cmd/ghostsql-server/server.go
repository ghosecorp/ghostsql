package main

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/ghosecorp/ghostsql/internal/protocol/pg"
	"github.com/ghosecorp/ghostsql/internal/storage"
)

// Server represents the GhostSQL network server
type Server struct {
	db   *storage.Database
	port int
}

// NewServer creates a new networked server
func NewServer(db *storage.Database, port int) *Server {
	return &Server{
		db:   db,
		port: port,
	}
}

// Start begins listening for TCP connections
func (s *Server) Start() error {
	addr := fmt.Sprintf(":%d", s.port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}
	defer listener.Close()

	s.db.Logger.Info("GhostSQL server listening on %s", addr)

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		s.db.Logger.Info("Shutting down server...")
		listener.Close()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			// Check if listener was closed
			select {
			case <-sigChan:
				return nil
			default:
				s.db.Logger.Error("Accept error: %v", err)
				continue
			}
		}

		// Handle connection in a new goroutine
		go s.handleConnection(conn)
	}
}

// handleConnection manages a single client session
func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	
	// Generate a unique session ID based on remote address
	sessionID := conn.RemoteAddr().String()
	session := s.db.SessionMgr.CreateSession(sessionID)
	defer s.db.SessionMgr.CloseSession(sessionID)

	// Set default database
	session.SetDatabase("ghostsql")

	s.db.Logger.Info("New connection from %s (Session: %s)", sessionID, sessionID)

	// Initialize PG protocol handler
	handler := pg.NewHandler(conn, s.db, session)
	if err := handler.Handle(); err != nil {
		s.db.Logger.Error("Session %s error: %v", sessionID, err)
	}
	
	s.db.Logger.Info("Connection closed: %s", sessionID)
}
