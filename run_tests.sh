#!/bin/bash
#
# VolGuard 3.0 - Comprehensive Test Runner
# Usage: ./run_tests.sh [unit|stress|chaos|all|quick|ci]
#

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
TEST_TYPE="${1:-all}"
PARALLEL_WORKERS="${PARALLEL_WORKERS:-auto}"
COVERAGE_THRESHOLD=85

# Functions
print_header() {
    echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║${NC}  VolGuard 3.0 - Test Suite Runner                       ${BLUE}║${NC}"
    echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
    echo ""
}

print_section() {
    echo -e "${YELLOW}━━━ $1 ━━━${NC}"
}

print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

print_info() {
    echo -e "${BLUE}ℹ${NC} $1"
}

check_dependencies() {
    print_section "Checking Dependencies"
    
    # Check Python
    if ! command -v python3 &> /dev/null; then
        print_error "Python 3 not found"
        exit 1
    fi
    print_success "Python $(python3 --version | cut -d' ' -f2) found"
    
    # Check pytest
    if ! python3 -c "import pytest" &> /dev/null; then
        print_error "pytest not installed"
        echo "Install with: pip install -r requirements-test.txt"
        exit 1
    fi
    print_success "pytest installed"
    
    # Check volguard.py exists
    if [ ! -f "volguard.py" ]; then
        print_error "volguard.py not found in current directory"
        exit 1
    fi
    print_success "volguard.py found"
    
    echo ""
}

setup_environment() {
    print_section "Setting Up Test Environment"
    
    # Set environment variables
    export VG_DRY_RUN=TRUE
    export VG_ENV=TEST
    export UPSTOX_ACCESS_TOKEN=test_token
    export TELEGRAM_BOT_TOKEN=test_bot_token
    export TELEGRAM_CHAT_ID=test_chat_id
    export VG_DB_PATH=/tmp/test_volguard.db
    export VG_LOG_DIR=/tmp/volguard_logs
    
    # Create directories
    mkdir -p /tmp/volguard_logs
    mkdir -p test-results
    
    # Clean up old test databases
    rm -f /tmp/test_*.db /tmp/*_test_db.db
    
    print_success "Environment configured"
    print_info "Dry Run Mode: ENABLED"
    print_info "Log Directory: /tmp/volguard_logs"
    echo ""
}

run_unit_tests() {
    print_section "Running Unit Tests"
    
    pytest test_unit.py \
        -v \
        --cov=volguard \
        --cov-report=html:htmlcov-unit \
        --cov-report=term-missing \
        --cov-report=xml:coverage-unit.xml \
        --junit-xml=test-results/unit.xml \
        --tb=short \
        --durations=10 \
        -n ${PARALLEL_WORKERS} \
        || { print_error "Unit tests failed"; return 1; }
    
    print_success "Unit tests passed"
    echo ""
}

run_stress_tests() {
    print_section "Running Stress Tests"
    
    print_info "This may take 10-20 minutes..."
    
    pytest test_stress.py \
        -v \
        -s \
        -m stress \
        --junit-xml=test-results/stress.xml \
        --tb=short \
        --durations=20 \
        --timeout=300 \
        || { print_error "Stress tests failed"; return 1; }
    
    print_success "Stress tests passed"
    echo ""
}

run_chaos_tests() {
    print_section "Running Chaos Tests"
    
    print_info "This may take 5-10 minutes..."
    
    pytest test_chaos.py \
        -v \
        -s \
        -m chaos \
        --junit-xml=test-results/chaos.xml \
        --tb=short \
        --timeout=180 \
        || { print_error "Chaos tests failed"; return 1; }
    
    print_success "Chaos tests passed"
    echo ""
}

run_quick_tests() {
    print_section "Running Quick Test Suite (Unit Tests Only)"
    
    pytest test_unit.py \
        -v \
        --tb=short \
        -x \
        --maxfail=3 \
        -n ${PARALLEL_WORKERS} \
        || { print_error "Quick tests failed"; return 1; }
    
    print_success "Quick tests passed"
    echo ""
}

run_ci_tests() {
    print_section "Running CI Test Suite"
    
    # Lint check
    print_info "Running code quality checks..."
    if command -v flake8 &> /dev/null; then
        flake8 volguard.py --count --select=E9,F63,F7,F82 --show-source --statistics || true
    fi
    
    # Unit tests with coverage
    pytest test_unit.py \
        -v \
        --cov=volguard \
        --cov-report=html:htmlcov-ci \
        --cov-report=term \
        --cov-report=xml:coverage-ci.xml \
        --junit-xml=test-results/ci.xml \
        --tb=line \
        -n ${PARALLEL_WORKERS} \
        || { print_error "CI tests failed"; return 1; }
    
    # Check coverage threshold
    COVERAGE=$(python3 -c "import xml.etree.ElementTree as ET; tree = ET.parse('coverage-ci.xml'); print(float(tree.getroot().attrib['line-rate']) * 100)")
    COVERAGE_INT=${COVERAGE%.*}
    
    if [ "$COVERAGE_INT" -lt "$COVERAGE_THRESHOLD" ]; then
        print_error "Coverage ${COVERAGE_INT}% is below threshold ${COVERAGE_THRESHOLD}%"
        return 1
    fi
    
    print_success "CI tests passed with ${COVERAGE_INT}% coverage"
    echo ""
}

generate_report() {
    print_section "Generating Test Report"
    
    echo "Test Results Summary"
    echo "===================="
    echo ""
    
    if [ -f "test-results/unit.xml" ]; then
        UNIT_TESTS=$(grep -o 'tests="[0-9]*"' test-results/unit.xml | head -1 | grep -o '[0-9]*')
        UNIT_FAILURES=$(grep -o 'failures="[0-9]*"' test-results/unit.xml | head -1 | grep -o '[0-9]*')
        echo "Unit Tests:    $UNIT_TESTS run, $UNIT_FAILURES failed"
    fi
    
    if [ -f "test-results/stress.xml" ]; then
        STRESS_TESTS=$(grep -o 'tests="[0-9]*"' test-results/stress.xml | head -1 | grep -o '[0-9]*')
        STRESS_FAILURES=$(grep -o 'failures="[0-9]*"' test-results/stress.xml | head -1 | grep -o '[0-9]*')
        echo "Stress Tests:  $STRESS_TESTS run, $STRESS_FAILURES failed"
    fi
    
    if [ -f "test-results/chaos.xml" ]; then
        CHAOS_TESTS=$(grep -o 'tests="[0-9]*"' test-results/chaos.xml | head -1 | grep -o '[0-9]*')
        CHAOS_FAILURES=$(grep -o 'failures="[0-9]*"' test-results/chaos.xml | head -1 | grep -o '[0-9]*')
        echo "Chaos Tests:   $CHAOS_TESTS run, $CHAOS_FAILURES failed"
    fi
    
    echo ""
    
    if [ -d "htmlcov-unit" ]; then
        print_info "Coverage report: htmlcov-unit/index.html"
    fi
    
    if [ -d "htmlcov-ci" ]; then
        print_info "Coverage report: htmlcov-ci/index.html"
    fi
    
    echo ""
}

cleanup() {
    print_section "Cleaning Up"
    
    # Remove test databases
    rm -f /tmp/test_*.db /tmp/*_test_db.db 2>/dev/null || true
    
    # Remove old logs
    rm -rf /tmp/volguard_logs/*.log 2>/dev/null || true
    
    print_success "Cleanup complete"
    echo ""
}

show_usage() {
    cat << EOF
Usage: ./run_tests.sh [TYPE]

Test Types:
  unit      - Run unit tests only (fast, ~2-5 minutes)
  stress    - Run stress tests only (slow, ~10-20 minutes)
  chaos     - Run chaos tests only (moderate, ~5-10 minutes)
  all       - Run all test suites (slow, ~20-30 minutes)
  quick     - Run quick unit tests (very fast, ~1 minute)
  ci        - Run CI pipeline tests (moderate, ~3-5 minutes)

Examples:
  ./run_tests.sh unit          # Run unit tests
  ./run_tests.sh all           # Run everything
  ./run_tests.sh quick         # Quick check before commit

Environment Variables:
  PARALLEL_WORKERS=N   - Number of parallel test workers (default: auto)

EOF
}

# Main execution
main() {
    print_header
    
    case "$TEST_TYPE" in
        unit)
            check_dependencies
            setup_environment
            run_unit_tests
            EXIT_CODE=$?
            ;;
        stress)
            check_dependencies
            setup_environment
            run_stress_tests
            EXIT_CODE=$?
            ;;
        chaos)
            check_dependencies
            setup_environment
            run_chaos_tests
            EXIT_CODE=$?
            ;;
        all)
            check_dependencies
            setup_environment
            run_unit_tests && run_stress_tests && run_chaos_tests
            EXIT_CODE=$?
            ;;
        quick)
            check_dependencies
            setup_environment
            run_quick_tests
            EXIT_CODE=$?
            ;;
        ci)
            check_dependencies
            setup_environment
            run_ci_tests
            EXIT_CODE=$?
            ;;
        help|--help|-h)
            show_usage
            exit 0
            ;;
        *)
            print_error "Unknown test type: $TEST_TYPE"
            show_usage
            exit 1
            ;;
    esac
    
    generate_report
    cleanup
    
    echo ""
    if [ $EXIT_CODE -eq 0 ]; then
        echo -e "${GREEN}╔════════════════════════════════════════════════════════════╗${NC}"
        echo -e "${GREEN}║${NC}  ✓ All Tests Passed Successfully!                       ${GREEN}║${NC}"
        echo -e "${GREEN}╚════════════════════════════════════════════════════════════╝${NC}"
    else
        echo -e "${RED}╔════════════════════════════════════════════════════════════╗${NC}"
        echo -e "${RED}║${NC}  ✗ Some Tests Failed - Check Output Above               ${RED}║${NC}"
        echo -e "${RED}╚════════════════════════════════════════════════════════════╝${NC}"
    fi
    echo ""
    
    exit $EXIT_CODE
}

# Run main function
main
