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

    # Setup LIBCLANG_PATH with better detection
    if [ -z "${LIBCLANG_PATH:-}" ]; then
        log_info "Searching for libclang..."
        
        # More comprehensive search paths
        POSSIBLE_PATHS=(
            "/usr/lib/llvm-18/lib"
            "/usr/lib/llvm-17/lib"
            "/usr/lib/llvm-16/lib"
            "/usr/lib/llvm-15/lib"
            "/usr/lib/llvm-14/lib"
            "/usr/lib/llvm-13/lib"
            "/usr/lib/llvm-12/lib"
            "/usr/lib/llvm-11/lib"
            "/usr/lib/llvm-10/lib"
            "/usr/lib/x86_64-linux-gnu"
            "/usr/lib64"
            "/usr/lib"
            "/usr/local/lib"
            "/opt/homebrew/lib"
        )

        # If llvm-config is available, add its path first
        if command -v llvm-config >/dev/null 2>&1; then
            LLVM_LIB_DIR="$(llvm-config --libdir)"
            POSSIBLE_PATHS=("$LLVM_LIB_DIR" "${POSSIBLE_PATHS[@]}")
            log_info "Found llvm-config, trying: $LLVM_LIB_DIR"
        fi

        # Function to check if libclang files exist in a directory
        check_libclang_in_path() {
            local path="$1"
            if [ -d "$path" ]; then
                # Check for the specific files that bindgen needs (not just libclang-cpp)
                local found_files=()
                for pattern in "libclang.so*" "libclang-*.so*" "libclang.dylib*"; do
                    if ls "$path"/$pattern >/dev/null 2>&1; then
                        found_files+=($(ls "$path"/$pattern))
                    fi
                done
                
                if [ ${#found_files[@]} -gt 0 ]; then
                    log_info "Found libclang files in $path:"
                    for file in "${found_files[@]}"; do
                        log_info "  - $(basename "$file")"
                    done
                    return 0
                else
                    # Check if only libclang-cpp exists (which won't work for bindgen)
                    if ls "$path"/libclang-cpp.so* >/dev/null 2>&1; then
                        log_warning "Found libclang-cpp in $path, but bindgen needs libclang.so"
                    fi
                fi
            fi
            return 1
        }

        # Find the first valid path that contains libclang
        FOUND_PATH=""
        for path in "${POSSIBLE_PATHS[@]}"; do
            if check_libclang_in_path "$path"; then
                FOUND_PATH="$path"
                break
            fi
        done

        if [ -n "$FOUND_PATH" ]; then
            export LIBCLANG_PATH="$FOUND_PATH"
            log_success "Using libclang from: $LIBCLANG_PATH"
        else
            log_error "libclang.so not found! You have libclang-cpp but bindgen needs libclang.so"
            log_error ""
            log_error "Please install the libclang development package:"
            log_error "  Ubuntu/Debian: sudo apt-get install libclang-dev"
            log_error "  Fedora/RHEL:   sudo dnf install clang-devel"
            log_error "  Arch Linux:    sudo pacman -S clang"
            log_error ""
            log_error "This will install libclang.so which bindgen requires."
            log_error ""
            log_error "Or find libclang.so manually and set:"
            log_error "  export LIBCLANG_PATH=/path/to/directory/containing/libclang.so"
            exit 1
        fi
    else
        log_info "Using LIBCLANG_PATH from environment: $LIBCLANG_PATH"
        
        # Verify the provided path actually has libclang.so (not just libclang-cpp)
        if [ -d "$LIBCLANG_PATH" ]; then
            if ls "$LIBCLANG_PATH"/libclang.so* >/dev/null 2>&1; then
                log_success "Verified libclang.so found in $LIBCLANG_PATH"
            else
                log_warning "No libclang.so files found in specified LIBCLANG_PATH: $LIBCLANG_PATH"
                log_warning "Contents of $LIBCLANG_PATH:"
                ls -la "$LIBCLANG_PATH" | grep -E "(clang|llvm)" || log_warning "  No clang/llvm related files found"
                if ls "$LIBCLANG_PATH"/libclang-cpp.so* >/dev/null 2>&1; then
                    log_warning "Found libclang-cpp.so but bindgen needs libclang.so"
                    log_warning "Please install: sudo apt-get install libclang-dev"
                fi
            fi
        else
            log_warning "LIBCLANG_PATH directory does not exist: $LIBCLANG_PATH"
        fi
    fi

    # Additional environment variables that might help
    if [ -n "${LIBCLANG_PATH:-}" ]; then
        # Some bindgen versions also look at these
        export CLANG_PATH="$(dirname "$LIBCLANG_PATH")/bin"
        export LLVM_CONFIG_PATH="$(which llvm-config || echo '')"
        
        # Add to LD_LIBRARY_PATH to ensure runtime linking works
        export LD_LIBRARY_PATH="${LIBCLANG_PATH}:${LD_LIBRARY_PATH:-}"
    fi

    # Enable backtrace for better error messages in development
    if [[ "$BUILD_TYPE" == "debug" ]]; then
        export RUST_BACKTRACE=1
        # Also enable debug output for build scripts when debugging
        export CARGO_PROFILE_DEV_BUILD_OVERRIDE_DEBUG=true
    fi

    # Set cargo flags based on verbosity
    CARGO_FLAGS=""
    if [[ "$VERBOSE" == "true" ]]; then
        CARGO_FLAGS="--verbose"
    fi

    # Print environment summary
    log_info "Environment summary:"
    log_info "  LIBCLANG_PATH: ${LIBCLANG_PATH:-'not set'}"
    log_info "  CLANG_PATH: ${CLANG_PATH:-'not set'}"
    log_info "  LLVM_CONFIG_PATH: ${LLVM_CONFIG_PATH:-'not set'}"
    log_info "  RUST_BACKTRACE: ${RUST_BACKTRACE:-'not set'}"

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
