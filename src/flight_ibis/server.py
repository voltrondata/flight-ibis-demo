import click
import pyarrow as pa
import pyarrow.flight
import pyarrow.parquet
import json
from munch import Munch, munchify
from datetime import datetime, timezone, timedelta
from pathlib import Path
import ibis
import duckdb
import jwt
import uuid
import os
from OpenSSL import crypto
from . import __version__ as flight_server_version
from .config import get_logger, logging, DUCKDB_DB_FILE, DUCKDB_THREADS, DUCKDB_MEMORY_LIMIT
from .data_logic_ibis import get_golden_rule_facts

# Constants
LOCALHOST_IP_ADDRESS: str = "0.0.0.0"
LOCALHOST: str = "localhost"
MAX_THREADS: int = 11
BEGINNING_OF_TIME: datetime = datetime(year=1775, month=11, day=10)
END_OF_TIME: datetime = datetime(year=9999, month=12, day=25)  # Merry last Christmas!
PYARROW_UNKNOWN: int = -1
JWT_ISS = "Flight Ibis"
JWT_AUD = "Flight Ibis"


class TokenServerAuthHandler(pa.flight.ServerAuthHandler):
    @property
    def class_name(self):
        return self.__class__.__name__

    def __init__(self,
                 creds: dict,
                 cert: bytes,
                 key: bytes,
                 logger):
        super().__init__()
        self.creds = creds

        # Extract the public key from the certificate
        pub_key = crypto.load_certificate(type=crypto.FILETYPE_PEM, buffer=cert).get_pubkey()
        self.public_key = crypto.dump_publickey(type=crypto.FILETYPE_PEM, pkey=pub_key)

        self.private_key = key
        self.logger = logger

    def authenticate(self, outgoing, incoming):
        username = incoming.read().decode()
        password = incoming.read().decode()

        if username in self.creds and self.creds[username] == password:
            # Create a JWT and sign it with our private key
            token = jwt.encode(payload=dict(jti=str(uuid.uuid4()),
                                            iss=JWT_ISS,
                                            sub=username,
                                            aud=JWT_AUD,
                                            iat=datetime.utcnow(),
                                            nbf=datetime.utcnow() - timedelta(minutes=1),
                                            exp=datetime.now(tz=timezone.utc) + timedelta(hours=24),
                                            ),
                               key=self.private_key,
                               algorithm="RS256"
                               )
            outgoing.write(token.encode())
            self.logger.info(msg=f"{self.class_name}.authenticate - User: '{username}' successfully authenticated - issued JWT.")
        else:
            error_message = f"{self.class_name}.authenticate - invalid username/password"
            self.logger.error(msg=error_message)
            raise pa.flight.FlightUnauthenticatedError(error_message)

    def is_valid(self, token):
        # Decode the jwt, validating the signature with the public key
        try:
            decoded_jwt = jwt.decode(jwt=token.decode(),
                                     key=self.public_key,
                                     algorithms=["RS256"],
                                     issuer=JWT_ISS,
                                     audience=JWT_AUD
                                     )
        except Exception as e:
            error_message = f"{self.class_name}.is_valid - jwt.decode failed with Exception: {str(e)}"
            self.logger.exception(msg=error_message)
            raise pa.flight.FlightError(error_message)
        else:
            subject = decoded_jwt.get("sub")
            self.logger.debug(msg=f"{self.class_name}.is_valid - JWT with subject: '{subject}' was successfully verified")
            return subject.encode()


class FlightServer(pa.flight.FlightServerBase):
    @property
    def class_name(self):
        return self.__class__.__name__

    def __init__(self,
                 host_uri: str,
                 location_uri: str,
                 database_file: Path,
                 duckdb_threads: int,
                 duckdb_memory_limit: str,
                 logger,
                 tls_certificates=None,
                 verify_client=False,
                 root_certificates=None,
                 auth_handler=None,
                 log_level: str = None,
                 log_file: str = None,
                 log_file_mode: str = None
                 ):
        self.logger = logger

        redacted_locals = {key: value for key, value in locals().items() if key not in ["tls_certificates",
                                                                                        "root_certificates"
                                                                                        ]
                           }
        self.logger.info(msg=f"Flight Server init args: {redacted_locals}")

        if not database_file.exists():
            raise RuntimeError(f"The specified database file: '{database_file.as_posix()}' does not exist, aborting.")

        super(FlightServer, self).__init__(
            location=host_uri,
            auth_handler=auth_handler,
            tls_certificates=tls_certificates,
            verify_client=verify_client,
            root_certificates=root_certificates
        )
        self.flights = {}
        self.tls_certificates = tls_certificates
        self.host_uri = host_uri
        self.location_uri = location_uri

        # Get an Ibis DuckDB connection
        self.ibis_connection = ibis.duckdb.connect(database=database_file,
                                                   threads=duckdb_threads,
                                                   memory_limit=duckdb_memory_limit,
                                                   read_only=True
                                                   )

        self.logger.info(f"Running Flight-Ibis server - version: {flight_server_version}")
        self.logger.info(f"Using PyArrow version: {pyarrow.__version__}")
        self.logger.info(f"Using Ibis version: {ibis.__version__}")
        self.logger.info(f"Using DuckDB version: {duckdb.__version__}")
        self.logger.info(f"Serving on {self.host_uri} (generated end-points will refer to location: {self.location_uri})")

    def _make_flight_info(self, command):
        self.logger.debug(msg=f"{self.class_name}._make_flight_info - command: {command}")

        command_munch: Munch = munchify(x=json.loads(command))

        command_munch.kwargs.total_hash_buckets = min(MAX_THREADS, command_munch.kwargs.num_threads)

        descriptor = pa.flight.FlightDescriptor.for_command(
            command=command_munch.command.encode('utf-8')
        )
        self.logger.debug(msg=f"{self.class_name}._make_flight_info - descriptor: {descriptor}")

        endpoints = []
        for i in range(1, (command_munch.kwargs.total_hash_buckets + 1)):
            command_munch.kwargs.hash_bucket_num = i
            endpoints.append(pa.flight.FlightEndpoint(json.dumps(command_munch.toDict()), [self.location_uri]))

        try:
            schema = get_golden_rule_facts(conn=self.ibis_connection,
                                           hash_bucket_num=99999,
                                           total_hash_buckets=1,
                                           min_date=BEGINNING_OF_TIME,
                                           max_date=BEGINNING_OF_TIME,
                                           schema_only=True,
                                           existing_logger=self.logger
                                           ).schema
        except Exception as e:
            error_message = f"{self.class_name}._make_flight_info - Error: {str(e)}"
            self.logger.error(msg=error_message)
            raise pa.flight.FlightError(error_message)
        else:
            return pyarrow.flight.FlightInfo(schema=schema,
                                             descriptor=descriptor,
                                             endpoints=endpoints,
                                             total_records=PYARROW_UNKNOWN,
                                             total_bytes=-PYARROW_UNKNOWN
                                             )

    def get_flight_info(self, context, descriptor):
        self.logger.info(msg=f"{self.class_name}.get_flight_info - called with context = {context}, descriptor = {descriptor}")
        flight_info = self._make_flight_info(descriptor.command.decode('utf-8'))
        self.logger.info(msg=(f"{self.class_name}.get_flight_info - with context = {context}, descriptor = {descriptor}"
                              f"- returning: FlightInfo ({dict(schema=flight_info.schema, endpoints=flight_info.endpoints)})"
                              )
                         )
        return flight_info

    def do_get(self, context, ticket):
        self.logger.info(msg=f"{self.class_name}.do_get - context = {context}, ticket = {ticket}")
        command_munch: Munch = munchify(x=json.loads(ticket.ticket.decode('utf-8')))

        command_kwargs = command_munch.kwargs

        if command_munch.command == "get_golden_rule_facts":
            try:
                golden_rule_kwargs = dict(conn=self.ibis_connection,
                                          hash_bucket_num=command_kwargs.hash_bucket_num,
                                          total_hash_buckets=command_kwargs.total_hash_buckets,
                                          min_date=datetime.fromisoformat(command_kwargs.min_date),
                                          max_date=datetime.fromisoformat(command_kwargs.max_date),
                                          schema_only=False,
                                          existing_logger=self.logger
                                          )
                self.logger.debug(msg=f"{self.class_name}.do_get - calling get_golden_rule_facts with args: {str(golden_rule_kwargs)}")
                dataset = get_golden_rule_facts(**golden_rule_kwargs)
            except Exception as e:
                error_message = f"{self.class_name}.get_flight_info - Exception: {str(e)}"
                self.logger.exception(msg=error_message)
                return pa.flight.FlightError(message=error_message)
            else:
                self.logger.info(msg=f"{self.class_name}.do_get - context: {context} - ticket: {ticket} - returning a dataset with row count: {dataset.num_rows}")
                return pa.flight.RecordBatchStream(dataset)
        else:
            error_message = f"{self.class_name}.do_get - Command: {command_munch.command} is not supported."
            self.logger.error(msg=error_message)
            raise pa.flight.FlightError(error_message)


@click.command()
@click.option(
    "--host",
    type=str,
    default=LOCALHOST_IP_ADDRESS,
    help="Address (or hostname) to listen on"
)
@click.option(
    "--location",
    type=str,
    default=LOCALHOST,
    help=("Address or hostname for TLS and endpoint generation.  This is needed if running the Flight server behind a load balancer and/or "
          "a reverse proxy"
          )
)
@click.option(
    "--port",
    type=int,
    default=8815,
    help="Port number to listen on"
)
@click.option(
    "--database-file",
    type=str,
    default=DUCKDB_DB_FILE.as_posix(),
    help="The DuckDB database file used for servicing data requests..."
)
@click.option(
    "--duckdb-threads",
    type=int,
    default=DUCKDB_THREADS,
    help="The number of threads to use for the DuckDB connection."
)
@click.option(
    "--duckdb-memory-limit",
    type=str,
    default=DUCKDB_MEMORY_LIMIT,
    help="The amount of memory to use for the DuckDB connection"
)
@click.option(
    "--tls",
    nargs=2,
    default=None,
    metavar=('CERTFILE', 'KEYFILE'),
    help="Enable transport-level security"
)
@click.option(
    "--verify-client/--no-verify-client",
    type=bool,
    default=False,
    show_default=True,
    required=True,
    help="enable mutual TLS and verify the client if True"
)
@click.option(
    "--mtls",
    type=str,
    default=None,
    help="If you provide verify-client, you must supply an MTLS CA Certificate file (public key only)"
)
@click.option(
    "--flight-username",
    type=str,
    default=os.getenv("FLIGHT_USERNAME"),
    required=False,
    show_default=False,
    help="If supplied, authentication will be required from clients to connect with this username"
)
@click.option(
    "--flight-password",
    type=str,
    default=os.getenv("FLIGHT_PASSWORD"),
    required=False,
    show_default=False,
    help="If supplied, authentication will be required from clients to connect with this password"
)
@click.option(
    "--log-level",
    type=click.Choice(["INFO", "DEBUG", "WARNING", "CRITICAL"], case_sensitive=False),
    default="INFO",
    help="The logging level to use"
)
@click.option(
    "--log-file",
    type=str,
    default=None,
    help="The log file to write to.  If None, will just log to stdout"
)
@click.option(
    "--log-file-mode",
    type=click.Choice(["a", "w"], case_sensitive=True),
    default="w",
    help="The log file mode, use value: a for 'append', and value: w to overwrite..."
)
def run_flight_server(host: str,
                      location: str,
                      port: int,
                      database_file: str,
                      duckdb_threads: int,
                      duckdb_memory_limit: str,
                      tls: list,
                      verify_client: bool,
                      mtls: str,
                      flight_username: str,
                      flight_password: str,
                      log_level: str,
                      log_file: str,
                      log_file_mode: str
                      ):
    tls_certificates = []
    scheme = "grpc+tcp"
    if tls:
        scheme = "grpc+tls"
        with open(tls[0], "rb") as cert_file:
            tls_cert_chain = cert_file.read()
        with open(tls[1], "rb") as key_file:
            tls_private_key = key_file.read()
        tls_certificates.append((tls_cert_chain, tls_private_key))

    root_certificates = None
    if verify_client:
        if not mtls:
            raise RuntimeError("You MUST provide a CA certificate public key file path if 'verify_client' is True, aborting.")

        if not tls:
            raise RuntimeError("TLS must be enabled in order to use MTLS, aborting.")

        with open(mtls, "rb") as mtls_ca_file:
            root_certificates = mtls_ca_file.read()

    logger = get_logger(filename=log_file,
                        filemode=log_file_mode,
                        logger_name="flight_server",
                        log_level=getattr(logging, log_level.upper())
                        )

    auth_handler = None
    if flight_username and flight_password:
        if not tls:
            raise RuntimeError("TLS must be enabled in order to use authentication, aborting.")
        auth_handler = TokenServerAuthHandler(creds={flight_username: flight_password},
                                              cert=tls_cert_chain,
                                              key=tls_private_key,
                                              logger=logger
                                              )

    host_uri = f"{scheme}://{host}:{port}"
    location_uri = f"{scheme}://{location}:{port}"
    server = FlightServer(host_uri=host_uri,
                          location_uri=location_uri,
                          database_file=Path(database_file),
                          duckdb_threads=duckdb_threads,
                          duckdb_memory_limit=duckdb_memory_limit,
                          logger=logger,
                          tls_certificates=tls_certificates,
                          verify_client=verify_client,
                          root_certificates=root_certificates,
                          auth_handler=auth_handler,
                          log_level=log_level,
                          log_file=log_file,
                          log_file_mode=log_file_mode
                          )
    try:
        server.serve()
    except Exception as e:
        server.logger.exception(msg=f"Flight server had exception: {str(e)}")
        raise
    finally:
        server.logger.info(msg="Flight server shutdown")
        logging.shutdown()


if __name__ == '__main__':
    run_flight_server()
