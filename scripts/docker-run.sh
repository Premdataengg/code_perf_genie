#!/bin/bash

# PySpark Performance Demo - Docker Runner Script
# This script provides easy commands to run the PySpark demo in Docker

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to show usage
show_usage() {
    echo "PySpark Performance Demo - Docker Runner"
    echo ""
    echo "Usage: $0 [COMMAND] [OPTIONS]"
    echo ""
    echo "Commands:"
    echo "  build           Build the Docker image"
    echo "  run             Run the demo (default: all queries)"
    echo "  run-interactive Run the demo in interactive mode"
    echo "  stop            Stop running containers"
    echo "  clean           Clean up Docker resources"
    echo "  logs            Show container logs"
    echo "  shell           Open shell in running container"
    echo "  help            Show this help message"
    echo ""
    echo "Options for run command:"
    echo "  --mode MODE     Query mode: all, anti_patterns, best_practices"
    echo "  --monitor       Enable performance monitoring"
    echo "  --verbose       Enable verbose output"
    echo "  --list          List available queries"
    echo ""
    echo "Examples:"
    echo "  $0 build"
    echo "  $0 run"
    echo "  $0 run --mode anti_patterns"
    echo "  $0 run --monitor --verbose"
    echo "  $0 run-interactive"
}

# Function to build Docker image
build_image() {
    print_info "Building Docker image..."
    
    if docker-compose build; then
        print_success "Docker image built successfully!"
    else
        print_error "Docker build failed!"
        exit 1
    fi
}

# Function to run the demo
run_demo() {
    local mode="all"
    local monitor=""
    local verbose=""
    local list=""
    
    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --mode)
                mode="$2"
                shift 2
                ;;
            --monitor)
                monitor="--monitor"
                shift
                ;;
            --verbose)
                verbose="--verbose"
                shift
                ;;
            --list)
                list="--list"
                shift
                ;;
            *)
                print_error "Unknown option: $1"
                show_usage
                exit 1
                ;;
        esac
    done
    
    print_info "Running PySpark demo with mode: $mode"
    
    # Build command
    local cmd="python3 main.py --mode $mode"
    if [[ -n "$monitor" ]]; then
        cmd="$cmd $monitor"
    fi
    if [[ -n "$verbose" ]]; then
        cmd="$cmd $verbose"
    fi
    if [[ -n "$list" ]]; then
        cmd="$cmd $list"
    fi
    
    # Run with docker-compose
    docker-compose run --rm pyspark-demo $cmd
}

# Function to run in interactive mode
run_interactive() {
    print_info "Starting interactive mode..."
    docker-compose run --rm -it pyspark-demo bash
}

# Function to stop containers
stop_containers() {
    print_info "Stopping containers..."
    docker-compose down
    print_success "Containers stopped!"
}

# Function to clean up
clean_up() {
    print_info "Cleaning up Docker resources..."
    docker-compose down --rmi all --volumes --remove-orphans
    docker system prune -f
    print_success "Cleanup completed!"
}

# Function to show logs
show_logs() {
    print_info "Showing container logs..."
    docker-compose logs -f pyspark-demo
}

# Function to open shell
open_shell() {
    print_info "Opening shell in container..."
    docker-compose exec pyspark-demo bash
}

# Main script logic
case "${1:-help}" in
    build)
        build_image
        ;;
    run)
        shift
        run_demo "$@"
        ;;
    run-interactive)
        run_interactive
        ;;
    stop)
        stop_containers
        ;;
    clean)
        clean_up
        ;;
    logs)
        show_logs
        ;;
    shell)
        open_shell
        ;;
    help|--help|-h)
        show_usage
        ;;
    *)
        print_error "Unknown command: $1"
        show_usage
        exit 1
        ;;
esac
