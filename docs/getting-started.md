# Getting Started

Follow these steps to get GhostSQL up and running on your machine.

## Quick Start with Docker (Recommended)

The easiest way to run GhostSQL is using our official Docker image.

### Using Docker Run
```bash
docker run -d -p 5433:5433 --name ghostsql ghosecorp/ghostsql:latest
```

### Using Docker Compose
If you have the GhostSQL repository cloned, you can simply run:
```bash
docker compose up -d
```

## Local Installation

### Prerequisites
*   **Go 1.25** or higher
*   **Make** (optional, but recommended)

### Build from Source
Clone the repository and build the binary:

```bash
git clone https://github.com/ghosecorp/ghostsql.git
cd ghostsql
make build
```

## Running the Server

Start the GhostSQL server on the default port (5433):

```bash
./bin/ghostsql-server
```

## Connecting with psql

You can use the standard PostgreSQL CLI to connect to GhostSQL:

```bash
psql -h localhost -p 5433 -d ghostsql
```

## First Steps

Once connected, try creating a table and inserting some data:

```sql
CREATE TABLE items (id INT, name TEXT);
INSERT INTO items VALUES (1, 'First Item');
SELECT * FROM items;
```
