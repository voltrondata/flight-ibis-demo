# Arrow Flight Ibis Server demo

## Setup (to run locally)

### Install package

#### 1. Clone the repo
```shell
git clone https://github.com/voltrondata/k8s-flight-demo

```

#### 2. Setup Python
Create a new Python 3.8+ virtual environment and install the Flight server/client demo with:
```shell
cd k8s-flight-demo

# Create the virtual environment
python3 -m venv ./venv

# Activate the virtual environment
. ./venv/bin/activate

# Upgrade pip, setuptools, and wheel
pip install --upgrade pip setuptools wheel

# Install the flight-ibis demo
pip install --editable .

```


#### 3. Create a sample TPC-H 1GB database (will take about 243MB of disk space due to compression)
```shell
. ./venv/bin/activate
flight-data-bootstrap

```
## Running the Flight Ibis Server and Client
We've provided 3 examples of how to run the Flight Ibis Server and Client - starting from the least to the most secure.

### Option 1) Running the Flight Ibis Server / Client demo without TLS (NOT secure)

#### Run the example
##### 1. Run the Flight Server
```shell
. ./venv/bin/activate
flight-server

```

##### 2. Open another terminal (leave the server running) - and run the Flight Client
```shell
. ./venv/bin/activate
flight-client

```

### Option 2) Running the Flight Ibis Server / Client demo with TLS (somewhat secure)

#### Run the example
##### 1. Generate a localhost TLS certificate keypair
```shell
. ./venv/bin/activate
flight-create-tls-keypair

```

##### 2. Run the Flight Server with TLS enabled (using the keypair created in step #1 above)
```shell
. ./venv/bin/activate
flight-server --tls=tls/server.crt tls/server.key

```

##### 3. Open another terminal (leave the server running) - and run the Flight Client with TLS enabled (trusting your cert created in step #1)
```shell
. ./venv/bin/activate
flight-client --host=localhost \
              --tls \
              --tls-roots=tls/server.crt

```

### Option 3) Running the Flight Ibis Server / Client demo with TLS and MTLS authentication (more secure)

#### Run the example
##### 1. Generate a localhost TLS certificate keypair
```shell
. ./venv/bin/activate
flight-create-tls-keypair

```

##### 2. Generate a Certificate Authority (CA) keypair - used to sign client certificates
```shell
. ./venv/bin/activate
flight-create-mtls-ca-keypair

```

##### 3. Generate a Client Certificate keypair (signed by the CA you just created in step #2 above)
```shell
. ./venv/bin/activate
flight-create-mtls-client-keypair

```

##### 4. Run the Flight Server with TLS and MTLS enabled (using the certificates created in the steps above)
```shell
. ./venv/bin/activate
flight-server --tls=tls/server.crt tls/server.key \
              --verify-client \
              --mtls=tls/ca.crt

```

##### 5. Open another terminal (leave the server running) - and run the Flight Client with TLS and MTLS enabled
```shell
. ./venv/bin/activate
flight-client --host=localhost \
              --tls \
              --tls-roots=tls/server.crt \
              --mtls=tls/client.crt tls/client.key
```
