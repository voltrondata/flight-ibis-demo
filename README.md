# Kubernetes Flight Server demo

# Setup (to run locally)

## Install package

### 1. Clone the repo
```shell
git clone https://github.com/voltrondata/k8s-flight-demo
```

### 2. Setup Python
Create a new Python 3.8+ virtual environment and install the Flight server/client demo with:
```shell
cd k8s-flight-demo

# Create the virtual environment
python3 -m venv ./venv

# Activate the virtual environment
. ./venv/bin/activate

# Upgrade pip, setuptools, and wheel
pip install --upgrade pip

# Install requirements
pip install --requirement requirements.txt

```

### 3. Create a sample TPC-H database
```shell
. ./venv/bin/activate
python ./flight_server/src/create_local_duckdb_database.py

```

## Run the example
### 1. Run the Flight Server
```shell
. ./venv/bin/activate
python ./flight_server/src/server.py

```

### 2. Open another terminal (leave the server running) - and run the Flight Client
```shell
. ./venv/bin/activate
python ./flight_client/src/client.py

```
