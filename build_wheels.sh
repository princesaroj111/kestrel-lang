#!/bin/bash

# Exit on error
set -e

# Create directory for wheels if it doesn't exist
mkdir -p wheels

echo "Building kestrel_core..."
cd packages/kestrel_core
rm -rf build dist *.egg-info
python -m build --wheel
cp dist/*.whl ../../wheels/
cd ../..

echo "Building kestrel_datasource_stixbundle..."
cd packages/kestrel_datasource_stixbundle
rm -rf build dist *.egg-info
python -m build --wheel
cp dist/*.whl ../../wheels/
cd ../..

echo "Building kestrel_datasource_stixshifter..."
cd packages/kestrel_datasource_stixshifter
rm -rf build dist *.egg-info
python -m build --wheel
cp dist/*.whl ../../wheels/
cd ../..

echo "Building kestrel_analytics_docker..."
cd packages/kestrel_analytics_docker
rm -rf build dist *.egg-info
python -m build --wheel
cp dist/*.whl ../../wheels/
cd ../..

echo "Building kestrel_analytics_python..."
cd packages/kestrel_analytics_python
rm -rf build dist *.egg-info
python -m build --wheel
cp dist/*.whl ../../wheels/
cd ../..

echo "Building kestrel_jupyter..."
cd packages/kestrel_jupyter
rm -rf build dist *.egg-info
python -m build --wheel
cp dist/*.whl ../../wheels/
cd ../..

echo "All wheels have been built and copied to ./wheels directory:"
ls -l wheels/