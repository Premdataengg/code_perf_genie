# PySpark Performance Demo - Makefile
# Provides convenient shortcuts for common operations

.PHONY: help build run run-anti run-best clean logs shell interactive test

# Default target
help:
	@echo "PySpark Performance Demo - Available Commands"
	@echo "============================================="
	@echo ""
	@echo "Docker Commands:"
	@echo "  build        Build Docker image"
	@echo "  run          Run demo with all queries"
	@echo "  run-anti     Run only anti-pattern queries"
	@echo "  run-best     Run only best practice queries"
	@echo "  run-monitor  Run with performance monitoring"
	@echo "  interactive  Run in interactive mode"
	@echo "  logs         Show container logs"
	@echo "  shell        Open shell in container"
	@echo "  clean        Clean up Docker resources"
	@echo ""
	@echo "Development Commands:"
	@echo "  test         Run unit tests"
	@echo "  quick-start  Run quick start script"
	@echo "  help         Show this help message"

# Docker commands
build:
	@echo "Building Docker image..."
	./scripts/docker-run.sh build

run:
	@echo "Running PySpark demo..."
	./scripts/docker-run.sh run

run-anti:
	@echo "Running anti-pattern queries..."
	./scripts/docker-run.sh run --mode anti_patterns

run-best:
	@echo "Running best practice queries..."
	./scripts/docker-run.sh run --mode best_practices

run-monitor:
	@echo "Running with performance monitoring..."
	./scripts/docker-run.sh run --monitor --verbose

interactive:
	@echo "Starting interactive mode..."
	./scripts/docker-run.sh run-interactive

logs:
	@echo "Showing container logs..."
	./scripts/docker-run.sh logs

shell:
	@echo "Opening shell in container..."
	./scripts/docker-run.sh shell

clean:
	@echo "Cleaning up Docker resources..."
	./scripts/docker-run.sh clean

# Development commands
test:
	@echo "Running unit tests..."
	python -m pytest tests/ -v

quick-start:
	@echo "Running quick start script..."
	./scripts/quick-start.sh

# Convenience targets
all: build run

dev: build interactive

# Show Docker status
status:
	@echo "Docker containers status:"
	docker-compose ps

# Show Docker images
images:
	@echo "Docker images:"
	docker images | grep pyspark
