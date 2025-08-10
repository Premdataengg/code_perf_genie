#!/bin/bash

# PySpark Performance Demo - Quick Start Script
# This script helps first-time users get started quickly

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

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

# Check if Docker is installed
check_docker() {
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed. Please install Docker first."
        echo "Visit: https://docs.docker.com/get-docker/"
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null; then
        print_error "Docker Compose is not installed. Please install Docker Compose first."
        echo "Visit: https://docs.docker.com/compose/install/"
        exit 1
    fi
    
    print_success "Docker and Docker Compose are installed!"
}

# Check if Docker daemon is running
check_docker_daemon() {
    if ! docker info &> /dev/null; then
        print_error "Docker daemon is not running. Please start Docker first."
        exit 1
    fi
    
    print_success "Docker daemon is running!"
}

# Welcome message
show_welcome() {
    echo ""
    echo "🚀 Welcome to PySpark SQL Performance Demo!"
    echo "=============================================="
    echo ""
    echo "This demo showcases Spark SQL performance optimization techniques"
    echo "through anti-patterns and best practices."
    echo ""
    echo "What you'll learn:"
    echo "• Performance anti-patterns to avoid"
    echo "• Best practices for optimal Spark performance"
    echo "• Real-world optimization techniques"
    echo ""
}

# Show next steps
show_next_steps() {
    echo ""
    echo "🎯 Next Steps:"
    echo "=============="
    echo ""
    echo "1. Build the Docker image:"
    echo "   ./scripts/docker-run.sh build"
    echo ""
    echo "2. Run the demo:"
    echo "   ./scripts/docker-run.sh run"
    echo ""
    echo "3. Run specific query types:"
    echo "   ./scripts/docker-run.sh run --mode anti_patterns"
    echo "   ./scripts/docker-run.sh run --mode best_practices"
    echo ""
    echo "4. Interactive mode:"
    echo "   ./scripts/docker-run.sh run-interactive"
    echo ""
    echo "5. View Spark UI:"
    echo "   Open http://localhost:4040 in your browser"
    echo ""
    echo "6. Get help:"
    echo "   ./scripts/docker-run.sh help"
    echo ""
}

# Main quick start logic
main() {
    show_welcome
    
    print_info "Checking prerequisites..."
    check_docker
    check_docker_daemon
    
    print_success "All prerequisites are met!"
    
    print_info "Ready to start the PySpark demo!"
    
    show_next_steps
    
    print_info "Would you like to build and run the demo now? (y/n)"
    read -r response
    
    if [[ "$response" =~ ^[Yy]$ ]]; then
        print_info "Building Docker image..."
        ./scripts/docker-run.sh build
        
        print_info "Running demo..."
        ./scripts/docker-run.sh run
        
        print_success "Demo completed! Check the output above for results."
    else
        print_info "No problem! You can run the demo anytime using the commands shown above."
    fi
}

# Run main function
main "$@"
