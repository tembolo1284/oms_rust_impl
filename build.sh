#!/bin/bash

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
BUILD_TYPE="${1:-debug}"
VERBOSE="${VERBOSE:-false}"
CLEAN="${CLEAN:-false}"

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

show_usage() {
    cat << EOF
Usage: $0 [OPTIONS] [BUILD_TYPE]

Build and development script for Trading OMS

BUILD_TYPE:
    debug       Build debug version (default)
    release     Build optimized release version
    test        Run all tests
    bench       Run benchmarks
    check       Run cargo check and clippy
    format      Format code with rustfmt
    clean       Clean build artifacts

OPTIONS:
    -h, --help      Show this help message
    -v, --verbose   Enable verbose output
    -c, --clean     Clean before building

Environment Variables:
    VERBOSE=true    Enable verbose output
    CLEAN=true      Clean before building
    RUST_LOG        Set logging level (debug, info, warn, error)

Examples:
    $0 debug        # Build debug version
    $0 release      # Build release version
    $0 test         # Run all tests
    CLEAN=true $0   # Clean and build debug
EOF
}

check_requirements() {
    log_info "Checking requirements..."
    
    # Check Rust installation
    if ! command -v rustc &> /dev/null; then
        log_error "Rust is not installed. Please install Rust from https://rustup.rs/"
        exit 1
    fi
    
    # Check cargo
    if ! command -v cargo &> /dev/null; then
        log_error "Cargo is not found. Please reinstall Rust with cargo."
        exit 1
    fi
    
    # Check minimum Rust version
    RUST_VERSION=$(rustc --version | cut -d' ' -f2)
    log_info "Rust version: $RUST_VERSION"
    
    # Check if we're in the right directory
    if [[ ! -f "Cargo.toml" ]]; then
        log_error "Cargo.toml not found. Please run this script from the project root."
        exit 1
    fi
    
    log_success "Requirements check passed"
}

setup_environment() {
    log_info "Setting up environment..."
    
    # Set default RUST_LOG if not set
    export RUST_LOG="${RUST_LOG:-info}"
    
    # Enable backtrace for better error messages in development
    if [[ "$BUILD_TYPE" == "debug" ]]; then
        export RUST_BACKTRACE=1
    fi
    
    # Set cargo flags based on verbosity
    CARGO_FLAGS=""
    if [[ "$VERBOSE" == "true" ]]; then
        CARGO_FLAGS="--verbose"
    fi
    
    log_success "Environment setup complete"
}

clean_build() {
    log_info "Cleaning build artifacts..."
    cargo clean
    
    # Clean target directory in client as well
    if [[ -d "client" ]]; then
        (cd client && cargo clean)
    fi
    
    log_success "Clean complete"
}

build_debug() {
    log_info "Building debug version..."
    cargo build $CARGO_FLAGS
    
    # Build client as well
    if [[ -d "client" ]]; then
        log_info "Building test client..."
        (cd client && cargo build $CARGO_FLAGS)
    fi
    
    log_success "Debug build complete"
}

build_release() {
    log_info "Building release version..."
    cargo build --release $CARGO_FLAGS
    
    # Build client as well
    if [[ -d "client" ]]; then
        log_info "Building test client (release)..."
        (cd client && cargo build --release $CARGO_FLAGS)
    fi
    
    log_success "Release build complete"
}

run_tests() {
    log_info "Running tests..."
    
    # Run unit tests
    log_info "Running unit tests..."
    cargo test $CARGO_FLAGS --lib
    
    # Run integration tests
    log_info "Running integration tests..."
    cargo test $CARGO_FLAGS --test '*'
    
    # Run doc tests
    log_info "Running doc tests..."
    cargo test $CARGO_FLAGS --doc
    
    # Test client as well
    if [[ -d "client" ]]; then
        log_info "Testing client..."
        (cd client && cargo test $CARGO_FLAGS)
    fi
    
    log_success "All tests passed"
}

run_benchmarks() {
    log_info "Running benchmarks..."
    cargo bench $CARGO_FLAGS
    log_success "Benchmarks complete"
}

run_checks() {
    log_info "Running cargo check..."
    cargo check $CARGO_FLAGS --all-targets
    
    log_info "Running clippy..."
    cargo clippy $CARGO_FLAGS --all-targets -- -D warnings
    
    # Check client as well
    if [[ -d "client" ]]; then
        log_info "Checking client..."
        (cd client && cargo check $CARGO_FLAGS)
        (cd client && cargo clippy $CARGO_FLAGS -- -D warnings)
    fi
    
    log_success "All checks passed"
}

format_code() {
    log_info "Formatting code..."
    cargo fmt --all
    
    # Format client as well
    if [[ -d "client" ]]; then
        (cd client && cargo fmt --all)
    fi
    
    log_success "Code formatting complete"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            show_usage
            exit 0
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        -c|--clean)
            CLEAN=true
            shift
            ;;
        debug|release|test|bench|check|format|clean)
            BUILD_TYPE="$1"
            shift
            ;;
        *)
            log_error "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Main execution
main() {
    log_info "Starting Trading OMS build script"
    log_info "Build type: $BUILD_TYPE"
    
    check_requirements
    setup_environment
    
    # Clean if requested
    if [[ "$CLEAN" == "true" ]]; then
        clean_build
    fi
    
    # Execute based on build type
    case "$BUILD_TYPE" in
        debug)
            build_debug
            ;;
        release)
            build_release
            ;;
        test)
            run_tests
            ;;
        bench)
            run_benchmarks
            ;;
        check)
            run_checks
            ;;
        format)
            format_code
            ;;
        clean)
            clean_build
            ;;
        *)
            log_error "Invalid build type: $BUILD_TYPE"
            show_usage
            exit 1
            ;;
    esac
    
    log_success "Build script completed successfully!"
}

main "$@"
