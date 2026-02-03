#!/bin/bash
# Quick start script for running the otap-dataflow engine

# Save the starting directory and restore it on exit (including errors)
STARTING_DIR="$PWD"
trap 'cd "$STARTING_DIR"' EXIT

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "Starting OTAP Dataflow Engine..."
echo ""

# Always rebuild to ensure features are enabled
echo "Building df_engine with required features..."
cd "${PROJECT_DIR}" && cargo build --features "condense-attributes-processor azure-monitor-exporter recordset-kql-processor experimental-tls"

if [ $? -ne 0 ]; then
    echo "Build failed. Exiting."
    exit 1
fi

echo "Starting engine..."
echo "Press Ctrl+C to stop the engine"
echo "================================"
echo ""

cd "${PROJECT_DIR}" || exit 1
./target/debug/df_engine --pipeline "${SCRIPT_DIR}/azmon-with-durablebuffer.yaml" --num-cores 1

if [ $? -ne 0 ]; then
    echo ""
    echo "Engine failed to start. Check the error message above."
    exit 1
fi
