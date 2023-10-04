#!/bin/bash

# Start the Flight Ibis Server - in the background...
flight-server &

# Sleep for a few seconds to allow the server to have time for initialization...
sleep 10

# Run the client
flight-client

RC=$?

if [ ${RC} -eq 0 ]; then
    echo "flight-client succeeded with return code ${RC}"
else
    echo "flight-client failed with return code ${RC}"
fi

# Stop the server...
kill %1

# Exit with the code of the python test...
exit ${RC}
