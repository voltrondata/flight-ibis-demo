#!/bin/bash

SCRIPT_DIR=$(dirname ${0})

# Generate TLS certificates if they are not present...
pushd "${SCRIPT_DIR}/.."
if [ ! -f tls/server.crt ]
then
   echo -n "Generating TLS certs...\n"
   flight-create-tls-keypair
fi
popd

# Start the server
flight-server
