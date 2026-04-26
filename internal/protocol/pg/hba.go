package pg

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)

// HBAMethod represents the authentication method
type HBAMethod string

const (
	MethodTrust    HBAMethod = "trust"
	MethodReject   HBAMethod = "reject"
	MethodPassword HBAMethod = "password" // scram-sha-256 or cleartext
)

// HBARule represents a single rule in pg_hba.conf
type HBARule struct {
	Type     string // "local", "host"
	Database string // "all", "ghostsql", etc.
	User     string // "all", "ghost", etc.
	Address  *net.IPNet
	Method   HBAMethod
}

// HBAConfig stores all loaded rules
type HBAConfig struct {
	Rules []HBARule
}

// LoadHBAConfig loads rules from a file
func LoadHBAConfig(path string) (*HBAConfig, error) {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			// Return default rules if file doesn't exist
			return DefaultHBAConfig(), nil
		}
		return nil, err
	}
	defer file.Close()

	config := &HBAConfig{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.Fields(line)
		if len(parts) < 5 {
			continue // Invalid line
		}

		rule := HBARule{
			Type:     parts[0],
			Database: parts[1],
			User:     parts[2],
			Method:   HBAMethod(parts[4]),
		}

		_, ipNet, err := net.ParseCIDR(parts[3])
		if err == nil {
			rule.Address = ipNet
		}

		config.Rules = append(config.Rules, rule)
	}

	return config, nil
}

// DefaultHBAConfig returns a permissive configuration
func DefaultHBAConfig() *HBAConfig {
	_, anyIP, _ := net.ParseCIDR("0.0.0.0/0")
	_, localIP, _ := net.ParseCIDR("127.0.0.1/32")
	return &HBAConfig{
		Rules: []HBARule{
			{Type: "host", Database: "all", User: "ghost", Address: anyIP, Method: MethodPassword},
			{Type: "host", Database: "all", User: "all", Address: localIP, Method: MethodTrust},
			{Type: "host", Database: "all", User: "all", Address: anyIP, Method: MethodPassword},
		},
	}
}

// Check verifies if a connection is allowed
func (c *HBAConfig) Check(remoteIP net.IP, dbName, user string) (HBAMethod, error) {
	for _, rule := range c.Rules {
		// Check database
		if rule.Database != "all" && rule.Database != dbName {
			continue
		}

		// Check user
		if rule.User != "all" && rule.User != user {
			continue
		}

		// Check address
		if rule.Address != nil && !rule.Address.Contains(remoteIP) {
			continue
		}

		return rule.Method, nil
	}

	return MethodReject, fmt.Errorf("no pg_hba.conf entry for host %s, user %s, database %s", remoteIP, user, dbName)
}
