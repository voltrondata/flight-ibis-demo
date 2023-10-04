FROM python:3.12

ARG TARGETPLATFORM
ARG TARGETARCH
ARG TARGETVARIANT
RUN printf "I'm building for TARGETPLATFORM=${TARGETPLATFORM}" \
    && printf ", TARGETARCH=${TARGETARCH}" \
    && printf ", TARGETVARIANT=${TARGETVARIANT} \n" \
    && printf "With uname -s : " && uname -s \
    && printf "and  uname -m : " && uname -m

# Update OS and install packages
RUN apt-get update --yes && \
    apt-get dist-upgrade --yes && \
    apt-get install --yes \
      screen \
      unzip \
      vim \
      zip

# Setup the AWS Client
WORKDIR /tmp

RUN case ${TARGETPLATFORM} in \
         "linux/amd64")  AWSCLI_FILE=https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip  ;; \
         "linux/arm64")  AWSCLI_FILE=https://awscli.amazonaws.com/awscli-exe-linux-aarch64.zip  ;; \
    esac && \
    curl "${AWSCLI_FILE}" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -f awscliv2.zip

# Install DuckDB CLI
ARG DUCKDB_VERSION="0.8.1"

RUN case ${TARGETPLATFORM} in \
         "linux/amd64")  DUCKDB_FILE=https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/duckdb_cli-linux-amd64.zip  ;; \
         "linux/arm64")  DUCKDB_FILE=https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/duckdb_cli-linux-aarch64.zip  ;; \
    esac && \
    curl --output /tmp/duckdb.zip --location ${DUCKDB_FILE} && \
    unzip /tmp/duckdb.zip -d /usr/bin && \
    rm /tmp/duckdb.zip

# Create an application user
RUN useradd app_user --create-home

ARG APP_DIR="/opt/flight_ibis"
RUN mkdir --parents ${APP_DIR} && \
    chown app_user:app_user ${APP_DIR}

USER app_user

WORKDIR ${APP_DIR}

# Setup a Python Virtual environment
ENV VIRTUAL_ENV=${APP_DIR}/venv
RUN python3 -m venv ${VIRTUAL_ENV} && \
    echo ". ${VIRTUAL_ENV}/bin/activate" >> ~/.bashrc

# Set the PATH so that the Python Virtual environment is referenced for subsequent RUN steps (hat tip: https://pythonspeed.com/articles/activate-virtualenv-dockerfile/)
ENV PATH="${VIRTUAL_ENV}/bin:${PATH}"

# Upgrade pip, setuptools, and wheel
RUN pip install --upgrade setuptools pip wheel

COPY --chown=app_user:app_user . .

# Install the Flight Ibis package and remove the source files
RUN pip install . && \
    rm -rf src build && \
    rm -f pyproject.toml

# Build the seed database
RUN flight-data-bootstrap

# Run a test to ensure that the server works...
RUN scripts/test_flight_ibis.sh

# Open Flight server port
EXPOSE 8815
