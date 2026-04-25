# Architecture

GhostSQL is built with a modular architecture to ensure scalability and ease of development.

## Core Components

### 1. Protocol Layer (`internal/protocol`)
Handles the PostgreSQL wire protocol. It is responsible for parsing incoming packets, managing connections, and formatting response messages (like `RowDescription` and `DataRow`).

### 2. Parser (`internal/parser`)
A custom SQL parser that converts SQL strings into an Abstract Syntax Tree (AST). It includes:
*   **Lexer**: Tokenizes the input string.
*   **Parser**: Validates syntax and builds the AST.

### 3. Executor (`internal/executor`)
The engine that executes the AST. It handles table scans, join logic, filtering (WHERE), sorting (ORDER BY), and vector similarity computations.

### 4. Storage Engine (`internal/storage`)
Manages on-disk data persistence.
*   **DatabaseInstance**: Represents a single database.
*   **Table**: Manages rows, schema, and indexes.
*   **Slotted Page Storage**: Efficiently manages binary data on disk.

## Flow of a Query

1.  **Client Connection**: Client connects via `psql`.
2.  **Protocol Handshake**: Protocol layer handles authentication and startup.
3.  **Request**: Client sends a query string.
4.  **Parse**: Parser converts string to AST.
5.  **Execute**: Executor processes the AST using the Storage Engine.
6.  **Response**: Protocol layer sends results back to the client.
