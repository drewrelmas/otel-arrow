#!/bin/bash
# Test script for sending sample syslog messages to the otap-dataflow engine

HOST=127.0.0.1

# Function to display usage
usage() {
    echo "Usage: $0 [MESSAGE_TYPE] [PORT]"
    echo ""
    echo "Arguments:"
    echo "  MESSAGE_TYPE  Type of message to send: 'syslog' or 'cef'"
    echo "  PORT          Target port number (default: 5514)"
    echo ""
    echo "Examples:"
    echo "  $0 syslog 5514"
    echo "  $0 cef 5515"
    echo "  $0              # Interactive mode"
    exit 1
}

# Check if nc (netcat) is available
if ! command -v nc &> /dev/null; then
    echo "Error: netcat (nc) is not installed. Please install it first:"
    echo "  Ubuntu/Debian: sudo apt-get install netcat"
    echo "  macOS: brew install netcat"
    echo "  RHEL/CentOS: sudo yum install nc"
    exit 1
fi

# Parse arguments or prompt for input
if [ $# -eq 0 ]; then
    # Interactive mode
    echo "Select message type:"
    echo "  1) syslog"
    echo "  2) cef"
    read -p "Enter choice (1 or 2): " choice
    
    case $choice in
        1) MESSAGE_TYPE="syslog" ;;
        2) MESSAGE_TYPE="cef" ;;
        *) echo "Invalid choice"; exit 1 ;;
    esac
    
    read -p "Enter port number (default: 5514): " PORT
    PORT=${PORT:-5514}
elif [ $# -eq 1 ]; then
    MESSAGE_TYPE=$1
    PORT=5514
elif [ $# -eq 2 ]; then
    MESSAGE_TYPE=$1
    PORT=$2
else
    usage
fi

# Validate message type
if [[ ! "$MESSAGE_TYPE" =~ ^(syslog|cef)$ ]]; then
    echo "Error: MESSAGE_TYPE must be 'syslog' or 'cef'"
    usage
fi

# Validate port number
if ! [[ "$PORT" =~ ^[0-9]+$ ]] || [ "$PORT" -lt 1 ] || [ "$PORT" -gt 65535 ]; then
    echo "Error: PORT must be a number between 1 and 65535"
    exit 1
fi

echo "Sending $MESSAGE_TYPE messages to ${HOST}:${PORT} (Press Ctrl+C to stop)..."
echo ""

# Generate random 5 character identifier for this test instance
TEST_ID=$(cat /dev/urandom | tr -dc 'a-z0-9' | fold -w 5 | head -n 1)
echo "Test instance ID: $TEST_ID"
echo ""

# Initialize index counter
INDEX=0

# Continuous loop
while true; do
    INDEX=$((INDEX + 1))
    
    # Send appropriate message based on type
    if [ "$MESSAGE_TYPE" = "syslog" ]; then
        echo "[test_id=$TEST_ID index=$INDEX] Sending regular syslog message with RFC3164 format..."
        echo "<134>$(date '+%b %d %H:%M:%S') securityhost myapp[1234]: User admin logged in from 10.0.0.1 successfully [test_id=$TEST_ID index=$INDEX]" | nc -q 1 ${HOST} ${PORT}
    else
        echo "[test_id=$TEST_ID index=$INDEX] Sending CEF (Common Event Format) message with RFC3164 format..."
        echo "<134>$(date '+%b %d %H:%M:%S') securityhost CEF:0|Security|threatmanager|1.0|100|worm successfully stopped|10|src=10.0.0.1 dst=2.1.2.2 spt=1232 dpt=80 proto=tcp act=blocked deviceDirection=inbound test_id=$TEST_ID index=$INDEX vendorspecificext1=value1 vendorspecificext2=value2" | nc -q 1 ${HOST} ${PORT}
    fi
    
    sleep 0.5
done
