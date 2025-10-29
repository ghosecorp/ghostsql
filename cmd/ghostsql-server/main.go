package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

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
	db, err := storage.Initialize()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize database: %v\n", err)
		os.Exit(1)
	}
	defer db.Shutdown()

	// Create executor
	exec := executor.NewExecutor(db)

	if *interactive {
		runInteractiveMode(exec, db)
	} else {
		db.Logger.Info("Server will listen on port %d (networking not yet implemented)", *port)
		db.Logger.Info("Press Ctrl+C to shutdown")

		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		<-sigChan

		fmt.Println("\nReceived shutdown signal...")
	}
}

func runInteractiveMode(exec *executor.Executor, db *storage.Database) {
	fmt.Println("GhostSQL Interactive Shell")
	fmt.Println("Type 'exit' or 'quit' to exit")
	fmt.Println()

	scanner := bufio.NewScanner(os.Stdin)

	for {
		// Show current database in prompt
		currentDB := db.CurrentDatabase
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

		if len(result.Rows) > 0 {
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
