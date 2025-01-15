#!/bin/bash -e

if [ ! -z $1 ];  then
   BRANCH=$1
   echo "Building $BRANCH"
   git checkout $BRANCH
else
   echo "Building current branch"
fi

rm -rf venv
python3.11 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install wheel build

rm -rf wheels
mkdir -p wheels

WHLS="kestrel_analytics_docker-1.8.2-py3-none-any.whl
kestrel_analytics_python-1.8.1-py3-none-any.whl
kestrel_core-1.8.2-py3-none-any.whl
kestrel_datasource_stixbundle-1.8.0-py3-none-any.whl
kestrel_datasource_stixshifter-1.8.5-py3-none-any.whl
kestrel_jupyter-1.8.7-py3-none-any.whl"

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

while IFS= read -r file; do
  ls wheels/$file
done <<< "$WHLS"

echo "All wheels have been built and copied to ./wheels directory"
