package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
	"encoding/binary"
)

// Role represents a PostgreSQL-compatible role
type Role struct {
	OID           int64     `json:"oid"`
	Name          string    `json:"name"`
	IsSuperuser   bool      `json:"is_superuser"`
	Inherits      bool      `json:"inherits"`
	CanCreateRole bool      `json:"can_create_role"`
	CanCreateDB   bool      `json:"can_create_db"`
	CanLogin      bool      `json:"can_login"`
	Replication   bool      `json:"replication"`
	BypassRLS     bool      `json:"bypass_rls"`
	ConnLimit     int       `json:"conn_limit"`
	PasswordHash  string    `json:"password_hash"`
	ValidUntil    time.Time `json:"valid_until"`
	
	// Internal privilege storage
	// ObjectKey -> Set of privileges (SELECT, INSERT, etc.)
	Privileges map[string]map[string]bool `json:"privileges"`
	
	// Role membership
	MemberOf []string `json:"member_of"`
}

// RoleStore handles persistence of roles
type RoleStore struct {
	Roles map[string]*Role
	mu    sync.RWMutex
	path  string
}

// NewRoleStore creates a new role store
func NewRoleStore(dataDir string) *RoleStore {
	return &RoleStore{
		Roles: make(map[string]*Role),
		path:  filepath.Join(dataDir, "global", "pg_authid"),
	}
}

// HashPassword creates a simple SHA-256 hash of the password
func HashPassword(password string) string {
	hash := sha256.Sum256([]byte(password))
	return hex.EncodeToString(hash[:])
}

// VerifyPassword checks if the password matches the hash
func (r *Role) VerifyPassword(password string) bool {
	if r.PasswordHash == "" {
		return true // No password required
	}
	return HashPassword(password) == r.PasswordHash
}

// Load loads roles from disk (binary)
func (rs *RoleStore) Load() error {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if _, err := os.Stat(rs.path); os.IsNotExist(err) {
		return nil // Initial boot
	}

	data, err := os.ReadFile(rs.path)
	if err != nil {
		return fmt.Errorf("failed to read roles file: %w", err)
	}

	// Simple binary parsing: [NumRoles (4 bytes)] followed by [Len (4 bytes)][JSON Data]...
	if len(data) < 4 {
		return nil // Empty file
	}

	numRoles := binary.BigEndian.Uint32(data[0:4])
	offset := 4
	
	newRoles := make(map[string]*Role)
	for i := uint32(0); i < numRoles; i++ {
		if offset+4 > len(data) {
			break
		}
		roleLen := binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
		
		if offset+int(roleLen) > len(data) {
			return fmt.Errorf("malformed binary roles file: unexpected EOF")
		}
		
		var role Role
		if err := json.Unmarshal(data[offset : offset+int(roleLen)], &role); err != nil {
			return fmt.Errorf("failed to unmarshal role at index %d: %w", i, err)
		}
		newRoles[role.Name] = &role
		offset += int(roleLen)
	}
	
	rs.Roles = newRoles
	return nil
}

// Save saves roles to disk (binary)
func (rs *RoleStore) Save() error {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	// Ensure global directory exists
	globalDir := filepath.Dir(rs.path)
	if err := os.MkdirAll(globalDir, 0755); err != nil {
		return fmt.Errorf("failed to create global directory: %w", err)
	}

	var buf []byte
	
	// [NumRoles (4 bytes)]
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(rs.Roles)))
	buf = append(buf, header...)
	
	for _, role := range rs.Roles {
		roleData, err := json.Marshal(role)
		if err != nil {
			return fmt.Errorf("failed to marshal role %s: %w", role.Name, err)
		}
		
		// [Len (4 bytes)][Data]
		lenBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(lenBuf, uint32(len(roleData)))
		buf = append(buf, lenBuf...)
		buf = append(buf, roleData...)
	}

	if err := os.WriteFile(rs.path, buf, 0644); err != nil {
		return fmt.Errorf("failed to write roles file: %w", err)
	}

	return nil
}

// GetRole retrieves a role by name
func (rs *RoleStore) GetRole(name string) (*Role, bool) {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	role, exists := rs.Roles[name]
	return role, exists
}

// CreateRole adds a new role
func (rs *RoleStore) CreateRole(role *Role) error {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if _, exists := rs.Roles[role.Name]; exists {
		return fmt.Errorf("role %s already exists", role.Name)
	}

	rs.Roles[role.Name] = role
	return nil
}

// DeleteRole removes a role by name
func (rs *RoleStore) DeleteRole(name string) error {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if _, exists := rs.Roles[name]; !exists {
		return fmt.Errorf("role %s does not exist", name)
	}
	delete(rs.Roles, name)
	return nil
}

// GrantPrivilege grants a privilege to a role on an object
func (rs *RoleStore) GrantPrivilege(roleName, objectKey, privilege string) error {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	role, exists := rs.Roles[roleName]
	if !exists {
		return fmt.Errorf("role %s does not exist", roleName)
	}

	if role.Privileges == nil {
		role.Privileges = make(map[string]map[string]bool)
	}

	if role.Privileges[objectKey] == nil {
		role.Privileges[objectKey] = make(map[string]bool)
	}

	role.Privileges[objectKey][privilege] = true
	return nil
}

// HasPrivilege checks if a role has a specific privilege on an object
func (rs *RoleStore) HasPrivilege(roleName, objectKey, privilege string) bool {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	role, exists := rs.Roles[roleName]
	if !exists {
		return false
	}

	if role.IsSuperuser {
		return true
	}

	if role.Privileges == nil {
		return false
	}

	// Check object-level privileges
	if objPrivs, ok := role.Privileges[objectKey]; ok {
		if objPrivs[privilege] {
			return true
		}
	}

	// Check group memberships (recursive)
	for _, groupName := range role.MemberOf {
		if rs.HasPrivilege(groupName, objectKey, privilege) {
			return true
		}
	}

	// Finally check the 'all' role (unless we ARE the 'all' role to avoid recursion)
	if roleName != "all" {
		if rs.HasPrivilege("all", objectKey, privilege) {
			return true
		}
	}

	return false
}
