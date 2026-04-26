# Build stage
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Copy dependency files
COPY go.mod ./
# COPY go.sum ./ (if exists)

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the server
RUN go build -o ghostsql-server ./cmd/ghostsql-server

# Run stage
FROM alpine:latest

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/ghostsql-server .

# Create data directory
RUN mkdir -p /app/data

# Expose PG port
EXPOSE 5433

# Start the server (non-interactive mode for Docker)
CMD ["./ghostsql-server", "-port", "5433", "-interactive=false"]
