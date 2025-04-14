#!/bin/bash
# Start ssh server
service ssh restart 

# Starting the services
bash start-services.sh

# Creating a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install packages
pip install wheel
pip install -r requirements.txt  

# Package the virtual env.
venv-pack -o .venv.tar.gz

# Pack libs to use inside jar
mkdir -p zip_libs
SITE_PACKAGES=$(python -c "import site; print(site.getsitepackages()[0])")
cp -r "$SITE_PACKAGES"/* zip_libs/
cd zip_libs
zip -r ../libs.zip .
cd ..

# Collect data
bash prepare_data.sh

# Run the indexer
bash index.sh /index/data

# Run the ranker
bash search.sh "James Dearden films"
bash search.sh "I want to find the American comedy created in 1916"
bash search.sh "Video game"