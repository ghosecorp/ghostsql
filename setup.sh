# Initialize the project
mkdir ghostsql
cd ghostsql
go mod init github.com/ghosecorp/ghostsql

# Create directory structure
mkdir -p cmd/{ghostsql-server,ghostsql-client}
mkdir -p internal/{storage,parser,executor,vector,metadata,transaction,wal,protocol,catalog,util}
mkdir -p pkg/ghostsql
mkdir -p tests/{integration,benchmarks}
mkdir -p docs examples scripts configs


# Create the initial files
touch internal/storage/datadir.go
touch internal/storage/types.go
touch internal/storage/page.go
touch internal/storage/database.go
touch internal/metadata/types.go
touch internal/metadata/store.go
touch internal/util/logger.go
touch internal/util/errors.go
touch cmd/ghostsql-server/main.go
touch Makefile
touch .gitignore
touch README.md



# Format code
make fmt

# Build the server
make build

# Run the server
make run