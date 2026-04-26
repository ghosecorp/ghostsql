package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"

	// "os/signal"
	"strings"

	"github.com/ghosecorp/ghostsql/internal/executor"
	"github.com/ghosecorp/ghostsql/internal/parser"
	"github.com/ghosecorp/ghostsql/internal/storage"
)

func main() {
	port := flag.Int("port", 5433, "Port to listen on")
	interactive := flag.Bool("interactive", true, "Run in interactive mode")
	flag.Parse()

	fmt.Println("╔═══════════════════════════════════════╗")
	fmt.Println("║         GhostSQL Database             ║")
	fmt.Println("║     High-Performance SQL + Vectors    ║")
	fmt.Println("╚═══════════════════════════════════════╝")
	fmt.Println()

	// Initialize database
	db, err := storage.Initialize("")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize database: %v\n", err)
		os.Exit(1)
	}
	defer db.Shutdown()

	// Check if setup is needed (only if ghost has default password and we are in interactive mode or forced)
	if _, exists := db.RoleStore.GetRole("ghost"); exists {
		ghost, _ := db.RoleStore.GetRole("ghost")
		if ghost.PasswordHash == storage.HashPassword("ghost") && *interactive {
			promptSetup(db)
		}
	}

	if *interactive {
		// Create a default session for interactive mode
		session := db.SessionMgr.CreateSession("local")
		session.SetDatabase("ghostsql") // Default DB

		exec := executor.NewExecutor(db, session)
		runInteractiveMode(exec, db, session)
	} else {
		srv := NewServer(db, *port)
		if err := srv.Start(); err != nil {
			fmt.Fprintf(os.Stderr, "Server error: %v\n", err)
			os.Exit(1)
		}
	}
}

func runInteractiveMode(exec *executor.Executor, db *storage.Database, session *storage.Session) {
	fmt.Println("GhostSQL Interactive Shell")
	fmt.Println("Type 'exit' or 'quit' to exit")
	fmt.Println()

	scanner := bufio.NewScanner(os.Stdin)

	for {
		// Show current database in prompt from session
		currentDB := session.GetDatabase()
		if currentDB == "" {
			currentDB = "none"
		}
		fmt.Printf("ghostsql[%s]> ", currentDB)

		if !scanner.Scan() {
			break
		}

		input := strings.TrimSpace(scanner.Text())

		if input == "" {
			continue
		}

		if input == "exit" || input == "quit" {
			fmt.Println("Goodbye!")
			break
		}

		// Parse and execute
		p := parser.NewParser(input)
		stmt, err := p.Parse()
		if err != nil {
			fmt.Printf("Parse error: %v\n", err)
			continue
		}

		result, err := exec.Execute(stmt)
		if err != nil {
			fmt.Printf("Execution error: %v\n", err)
			continue
		}

		// Display result
		if result.Message != "" {
			fmt.Println(result.Message)
		}

		if len(result.Columns) > 0 {
			printTable(result.Columns, result.Rows)
		}
	}
}

func printTable(columns []string, rows []storage.Row) {
	// Print header
	for _, col := range columns {
		fmt.Printf("%-20s", col)
	}
	fmt.Println()
	fmt.Println(strings.Repeat("-", 20*len(columns)))

	// Print rows
	for _, row := range rows {
		for _, col := range columns {
			val := row[col]

			// Handle Vector type specially
			if vec, ok := val.(*storage.Vector); ok {
				// Format as [0.1, 0.2, 0.3, ...]
				vecStr := "["
				for i, v := range vec.Values {
					if i > 0 {
						vecStr += ", "
					}
					vecStr += fmt.Sprintf("%.4g", v)
				}
				vecStr += "]"

				// Truncate if too long for display
				if len(vecStr) > 18 {
					vecStr = vecStr[:15] + "..."
				}
				fmt.Printf("%-20s", vecStr)
			} else {
				fmt.Printf("%-20v", val)
			}
		}
		fmt.Println()
	}
	fmt.Printf("\n%d row(s)\n\n", len(rows))
}

func promptSetup(db *storage.Database) {
	fmt.Println("--------------------------------------------------")
	fmt.Println("Initial Setup: Administrative Account")
	fmt.Println("The default superuser is ghost.")
	fmt.Println("Press Enter to use default password (ghost),")
	fmt.Println("or type a new password:")
	fmt.Print("Password: ")
	
	scanner := bufio.NewScanner(os.Stdin)
	if scanner.Scan() {
		pw := strings.TrimSpace(scanner.Text())
		if pw != "" {
			ghost, _ := db.RoleStore.GetRole("ghost")
			ghost.PasswordHash = storage.HashPassword(pw)
			db.RoleStore.Save()
			fmt.Println("Password updated successfully.")
		} else {
			fmt.Println("Using default password: ghost")
		}
	}
	fmt.Println("--------------------------------------------------")
}

