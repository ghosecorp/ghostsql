#!/bin/bash

# Cleanup old test data
rm -rf ./test_integration_data

# Build the server
go build -o ghostsql-server ./cmd/ghostsql-server

# Start the server in the background
./ghostsql-server -port 5433 -interactive=false &
SERVER_PID=$!

echo "GhostSQL started with PID $SERVER_PID"

# Wait for server to be ready
sleep 2

# Trap to kill server on exit
trap "kill $SERVER_PID; echo 'GhostSQL stopped'; exit" INT TERM EXIT

# Keep script running
wait $SERVER_PID
