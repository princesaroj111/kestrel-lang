#!/bin/bash

# Exit on error
set -e

echo "Cleaning distribution files..."

# Remove wheels directory in root if it exists
if [ -d "wheels" ]; then
    echo "Removing wheels directory..."
    rm -rf wheels
fi

# Clean each package directory
for pkg_dir in packages/*/; do
    if [ -d "$pkg_dir" ]; then
        echo "Cleaning $pkg_dir..."
        (
            cd "$pkg_dir"
            rm -rf build dist *.egg-info
        )
    fi
done

echo "Clean up complete!"