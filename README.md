Table Storage:
┌─────────────────────────────────────┐
│ Table Header Page (Page 0)          │
│ - Magic number                      │
│ - Table schema                      │
│ - Root B+tree page ID               │
│ - Row count, statistics             │
└─────────────────────────────────────┘
           ↓
┌─────────────────────────────────────┐
│ B+Tree Index Pages                  │
│ - Internal nodes (keys + page IDs)  │
│ - Leaf nodes (keys + row data)      │
└─────────────────────────────────────┘
           ↓
┌─────────────────────────────────────┐
│ Data Pages                          │
│ - Fixed/variable length rows        │
│ - Slotted page format               │
└─────────────────────────────────────┘

edwardsepiol@EdwardSepiol:/mnt/c/Users/anubr/anu_projects/ghosecorp/ghostsql$ make build
Building GhostSQL server...
go build -o bin/ghostsql-server ./cmd/ghostsql-server
edwardsepiol@EdwardSepiol:/mnt/c/Users/anubr/anu_projects/ghosecorp/ghostsql$ make run
Building GhostSQL server...
go build -o bin/ghostsql-server ./cmd/ghostsql-server
Starting GhostSQL server...
./bin/ghostsql-server
╔═══════════════════════════════════════╗
║         GhostSQL Database             ║
║     High-Performance SQL + Vectors    ║
╚═══════════════════════════════════════╝

[2025-10-27 19:11:47] [INFO] [GhostSQL] Initializing data directory...
[2025-10-27 19:11:47] [INFO] [GhostSQL] Loading databases from disk...
[2025-10-27 19:11:47] [INFO] [GhostSQL] Loaded 2 table(s) for database ghostsql
[2025-10-27 19:11:47] [INFO] [GhostSQL] Loaded 2 table(s) for database myapp
[2025-10-27 19:11:47] [INFO] [GhostSQL] Loaded 0 table(s) for database production
[2025-10-27 19:11:47] [INFO] [GhostSQL] Loaded 0 table(s) for database staging
[2025-10-27 19:11:47] [INFO] [GhostSQL] Database initialized at: /mnt/c/Users/anubr/anu_projects/ghosecorp/ghostsql/bin/data
[2025-10-27 19:11:47] [INFO] [GhostSQL] Loaded 4 database(s)
GhostSQL Interactive Shell
Type 'exit' or 'quit' to exit

ghostsql[ghostsql]> CREATE INDEX embeddings_idx ON embeddings USING HNSW (embedding) WITH (m=16, ef_construction=200);
CREATE INDEX embeddings_idx ON embeddings USING HNSW (m=16, ef_construction=200)
ghostsql[ghostsql]> SELECT id, text FROM embeddings ORDER BY COSINE_DISTANCE(embedding, [0.1, 0.2, 0.3, 0.4]) LIMIT 2;
id                  text                _distance
------------------------------------------------------------
1                   hello world         0.000000
3                   test vector         0.002035

2 row(s)

ghostsql[ghostsql]>


-- Create embeddings table
CREATE TABLE documents (
    id INT,
    title VARCHAR,
    content TEXT,
    embedding VECTOR
);

-- Insert documents with embeddings
INSERT INTO documents VALUES (1, 'AI Tutorial', 'Learn AI', [0.8, 0.2, 0.1, 0.05]);
INSERT INTO documents VALUES (2, 'ML Guide', 'Machine Learning', [0.85, 0.25, 0.15, 0.08]);
INSERT INTO documents VALUES (3, 'Cooking', 'Recipe book', [0.1, 0.7, 0.8, 0.2]);

-- Exact search (brute-force, 100% accurate)
SELECT title FROM documents 
ORDER BY COSINE_DISTANCE(embedding, [0.82, 0.22, 0.12, 0.06]) 
LIMIT 3;

-- Create HNSW index for fast approximate search
CREATE INDEX docs_idx ON documents USING HNSW (embedding) WITH (m=16, ef_construction=200);

-- Now 95-99% accurate but 100x faster!
SELECT title FROM documents 
ORDER BY COSINE_DISTANCE(embedding, [0.82, 0.22, 0.12, 0.06]) 
LIMIT 3;