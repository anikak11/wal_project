.PHONY: help build run test test-verbose test-coverage clean fmt vet lint install deps demo

# Variables
BINARY_NAME=wal_project
MAIN_PATH=./main.go
TEST_PATH=./...

# Default target
help:
	@echo "Available targets:"
	@echo "  make build          - Build the project"
	@echo "  make run            - Run the main program"
	@echo "  make test           - Run tests"
	@echo "  make test-verbose   - Run tests with verbose output"
	@echo "  make test-coverage  - Run tests with coverage report"
	@echo "  make fmt            - Format Go code"
	@echo "  make vet            - Run go vet"
	@echo "  make lint           - Run golangci-lint (if installed)"
	@echo "  make clean          - Remove build artifacts"
	@echo "  make install        - Install dependencies"
	@echo "  make deps           - Download dependencies"
	@echo "  make demo           - Run the demo program"

# Build the project
build:
	@echo "Building $(BINARY_NAME)..."
	@go build -o $(BINARY_NAME) $(MAIN_PATH)
	@echo "Build complete: $(BINARY_NAME)"

# Run the main program
run:
	@go run $(MAIN_PATH)

# Run tests
test:
	@echo "Running tests..."
	@go test $(TEST_PATH)

# Run tests with verbose output
test-verbose:
	@echo "Running tests with verbose output..."
	@go test -v $(TEST_PATH)

# Run tests with coverage
test-coverage:
	@echo "Running tests with coverage..."
	@go test -coverprofile=coverage.out $(TEST_PATH)
	@go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

# Format Go code
fmt:
	@echo "Formatting Go code..."
	@go fmt $(TEST_PATH)
	@echo "Formatting complete"

# Run go vet
vet:
	@echo "Running go vet..."
	@go vet $(TEST_PATH)
	@echo "Vet complete"

# Run golangci-lint (if installed)
lint:
	@echo "Running golangci-lint..."
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run; \
	else \
		echo "golangci-lint not installed. Install with: go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest"; \
	fi

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	@rm -f $(BINARY_NAME)
	@rm -f coverage.out coverage.html
	@go clean
	@echo "Clean complete"

# Install dependencies
install:
	@echo "Installing dependencies..."
	@go mod download
	@go mod tidy
	@echo "Dependencies installed"

# Download dependencies
deps:
	@echo "Downloading dependencies..."
	@go mod download
	@echo "Dependencies downloaded"

# Run the demo
demo: run

# All checks (format, vet, test)
check: fmt vet test
	@echo "All checks passed!"

