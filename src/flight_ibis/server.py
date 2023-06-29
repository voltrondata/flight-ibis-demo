import base64
import json
import os
import sys
import uuid
from datetime import datetime, timezone, timedelta
from functools import cached_property
from pathlib import Path

import click
import duckdb
import ibis
import jwt
import pyarrow as pa
import pyarrow.flight
import pyarrow.parquet
from OpenSSL import crypto
from munch import Munch, munchify
from pyarrow.flight import SchemaResult

from . import __version__ as flight_server_version
from .config import get_logger, logging, DUCKDB_DB_FILE, DUCKDB_THREADS, DUCKDB_MEMORY_LIMIT
from .constants import LOCALHOST_IP_ADDRESS, LOCALHOST, GRPC_TCP_SCHEME, GRPC_TLS_SCHEME, MAX_THREADS, BEGINNING_OF_TIME, PYARROW_UNKNOWN, JWT_ISS, JWT_AUD
from .data_logic_ibis import build_customer_order_summary_expr, build_golden_rules_ibis_expression, get_golden_rule_fact_batches


class BasicAuthServerMiddlewareFactory(pa.flight.ServerMiddlewareFactory):
    """
    Middleware that implements username-password authentication.

    Parameters
    ----------
    creds: Dict[str, str]
        A dictionary of username-password values to accept.
    """

    @cached_property
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

    def start_call(self, info, headers):
        """Validate credentials at the start of every call."""
        self.logger.debug(msg=f"{self.class_name}.start_call - called with args: {locals()}")
        # Search for the authentication header (case-insensitive)
        auth_header = None
        for header in headers:
            if header.lower() == "authorization":
                auth_header = headers[header][0]
                break

        if not auth_header:
            raise pa.flight.FlightUnauthenticatedError("No credentials supplied")

        # The header has the structure "AuthType TokenValue", e.g.
        # "Basic <encoded username+password>" or "Bearer <random token>".
        auth_type, _, value = auth_header.partition(" ")

        if auth_type == "Basic":
            # Initial "login". The user provided a username/password
            # combination encoded in the same way as HTTP Basic Auth.
            decoded = base64.b64decode(value).decode("utf-8")
            username, _, password = decoded.partition(':')
            if not password or password != self.creds.get(username):
                error_message = f"{self.class_name}.start_call - invalid username/password"
                self.logger.error(msg=error_message)
                raise pa.flight.FlightUnauthenticatedError(error_message)

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
            self.logger.info(msg=f"{self.class_name}.start_call - User: '{username}' successfully authenticated - issued JWT.")
            return BasicAuthServerMiddleware(token)
        elif auth_type == "Bearer":
            # An actual call. Validate the bearer token.
            try:
                decoded_jwt = jwt.decode(jwt=value,
                                         key=self.public_key,
                                         algorithms=["RS256"],
                                         issuer=JWT_ISS,
                                         audience=JWT_AUD
                                         )
            except Exception as e:
                raise pa.flight.FlightUnauthenticatedError("Invalid token")
            else:
                subject = decoded_jwt.get("sub")
                self.logger.debug(msg=f"{self.class_name}.start_call - JWT with subject: '{subject}' was successfully verified")
                return BasicAuthServerMiddleware(value)

        raise pa.flight.FlightUnauthenticatedError("No credentials supplied")


class BasicAuthServerMiddleware(pa.flight.ServerMiddleware):
    """Middleware that implements username-password authentication."""

    def __init__(self, token):
        self.token = token

    def sending_headers(self):
        """Return the authentication token to the client."""
        return {"authorization": f"Bearer {self.token}"}


class NoOpAuthHandler(pa.flight.ServerAuthHandler):
    """
    A handler that implements username-password authentication.

    This is required only so that the server will respond to the internal
    Handshake RPC call, which the client calls when authenticate_basic_token
    is called. Otherwise, it should be a no-op as the actual authentication is
    implemented in middleware.
    """

    def authenticate(self, outgoing, incoming):
        pass

    def is_valid(self, token):
        return ""


class FlightServer(pa.flight.FlightServerBase):
    @cached_property
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
                 middleware=None,
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
            middleware=middleware,
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
        self.customer_order_summary_expr = build_customer_order_summary_expr(conn=self.ibis_connection)
        self.golden_rules_ibis_expression = build_golden_rules_ibis_expression(conn=self.ibis_connection,
                                                                               customer_order_summary_expr=self.customer_order_summary_expr
                                                                               )

        self.logger.info(f"Running Flight-Ibis server - version: {flight_server_version}")
        self.logger.info(f"Using Python version: {sys.version}")
        self.logger.info(f"Using PyArrow version: {pyarrow.__version__}")
        self.logger.info(f"Using Ibis version: {ibis.__version__}")
        self.logger.info(f"Using DuckDB version: {duckdb.__version__}")
        self.logger.info(f"Database details:")
        self.logger.info(f"   Database file: {database_file.as_posix()}")
        self.logger.info(f"   Threads: {duckdb_threads}")
        self.logger.info(f"   Memory Limit: {duckdb_memory_limit}")
        self.logger.info(f"Serving on {self.host_uri} (generated end-points will refer to location: {self.location_uri})")

    @cached_property
    def schema(self) -> pyarrow.Schema:
        return get_golden_rule_fact_batches(golden_rules_ibis_expression=self.golden_rules_ibis_expression,
                                            hash_bucket_num=99999,
                                            total_hash_buckets=1,
                                            min_date=BEGINNING_OF_TIME,
                                            max_date=BEGINNING_OF_TIME,
                                            schema_only=True,
                                            existing_logger=self.logger
                                            ).schema

    def _make_flight_info(self, command_munch: Munch) -> pyarrow.flight.FlightInfo:
        self.logger.debug(msg=f"{self.class_name}._make_flight_info - was called with args: {locals()}")

        command_munch.kwargs.total_hash_buckets = min(MAX_THREADS, command_munch.kwargs.num_threads)

        descriptor = pa.flight.FlightDescriptor.for_command(
            command=command_munch.command.encode('utf-8')
        )
        self.logger.debug(msg=f"{self.class_name}._make_flight_info - descriptor: {descriptor}")

        endpoints = []
        for i in range(1, (command_munch.kwargs.total_hash_buckets + 1)):
            command_munch.kwargs.hash_bucket_num = i
            endpoints.append(pa.flight.FlightEndpoint(json.dumps(command_munch.toDict()), [self.location_uri]))

        return pyarrow.flight.FlightInfo(schema=self.schema,
                                         descriptor=descriptor,
                                         endpoints=endpoints,
                                         total_records=PYARROW_UNKNOWN,
                                         total_bytes=PYARROW_UNKNOWN
                                         )

    def _check_command(self, command: dict) -> Munch:
        self.logger.debug(msg=f"{self.class_name}._check_command - was called with args: {locals()}")
        command_munch: Munch = munchify(x=command)

        if command_munch.command != "get_golden_rule_facts":
            error_message = f"{self.class_name}.do_get - Command: {command_munch.command} is not supported."
            self.logger.error(msg=error_message)
            raise pa.flight.FlightError(error_message)
        else:
            return command_munch

    def _get_descriptor_command(self, descriptor) -> dict:
        self.logger.debug(msg=f"{self.class_name}._get_descriptor_command - was called with args: {locals()}")
        return json.loads(descriptor.command.decode('utf-8'))

    def _get_ticket_command(self, ticket) -> dict:
        self.logger.debug(msg=f"{self.class_name}._get_ticket_command - was called with args: {locals()}")
        return json.loads(ticket.ticket.decode('utf-8'))

    def get_flight_info(self, context, descriptor) -> pyarrow.flight.FlightInfo:
        self.logger.info(msg=f"{self.class_name}.get_flight_info - was called with args: {locals()}")
        command = self._get_descriptor_command(descriptor=descriptor)
        command_munch = self._check_command(command=command)

        if command_munch.command == "get_golden_rule_facts":
            self.logger.info(msg=f"{self.class_name}.get_flight_info - called with context = {context}, descriptor = {descriptor}")
            flight_info = self._make_flight_info(command_munch=command_munch)
            self.logger.info(msg=(f"{self.class_name}.get_flight_info - with context = {context}, descriptor = {descriptor}"
                                  f"- returning: FlightInfo ({dict(schema=flight_info.schema, endpoints=flight_info.endpoints)})"
                                  )
                             )
            return flight_info

    def get_schema(self, context, descriptor) -> pyarrow.Schema:
        self.logger.info(msg=f"{self.class_name}.get_schema - was called with args: {locals()}")
        command = self._get_descriptor_command(descriptor=descriptor)
        _ = self._check_command(command=command)
        self.logger.info(msg=(f"{self.class_name}.get_schema - with context = {context}, descriptor = {descriptor}"
                              f"- returning: Schema ({dict(schema=self.schema)})"
                              )
                         )
        return SchemaResult(self.schema)

    def do_get(self, context, ticket) -> pyarrow.flight.FlightDataStream:
        self.logger.info(msg=f"{self.class_name}.do_get - was called with args: {locals()}")

        try:
            command = self._get_ticket_command(ticket=ticket)
            command_munch = self._check_command(command=command)

            golden_rule_kwargs = dict(golden_rules_ibis_expression=self.golden_rules_ibis_expression,
                                      hash_bucket_num=command_munch.kwargs.hash_bucket_num,
                                      total_hash_buckets=command_munch.kwargs.total_hash_buckets,
                                      min_date=datetime.fromisoformat(command_munch.kwargs.min_date),
                                      max_date=datetime.fromisoformat(command_munch.kwargs.max_date),
                                      schema_only=False,
                                      existing_logger=self.logger
                                      )
            self.logger.debug(msg=f"{self.class_name}.do_get - calling get_golden_rule_facts with args: {str(golden_rule_kwargs)}")
            batch_reader = get_golden_rule_fact_batches(**golden_rule_kwargs)
        except Exception as e:
            error_message = f"{self.class_name}.get_flight_info - Exception: {str(e)}"
            self.logger.exception(msg=error_message)
            return pa.flight.FlightError(message=error_message)
        else:
            self.logger.info(msg=f"{self.class_name}.do_get - context: {context} - ticket: {ticket} - returning a PyArrow RecordBatchReader...")
            return pyarrow.flight.GeneratorStream(schema=batch_reader.schema, generator=batch_reader)

    def do_action(self, context, action) -> list:
        self.logger.info(msg=f"{self.class_name}.do_action - was called with args: {locals()}")
        if action.type == "who-am-i":
            return [context.peer_identity(), context.peer().encode("utf-8")]
        raise NotImplementedError


@click.command()
@click.option(
    "--host",
    type=str,
    default=os.getenv("FLIGHT_HOST", LOCALHOST_IP_ADDRESS),
    required=True,
    help="Address (or hostname) to listen on"
)
@click.option(
    "--location",
    type=str,
    default=os.getenv("FLIGHT_LOCATION", f"{LOCALHOST}:{os.getenv('FLIGHT_PORT', 8815)}"),
    required=True,
    help=("Address or hostname for TLS and endpoint generation.  This is needed if running the Flight server behind a load balancer and/or "
          "a reverse proxy"
          )
)
@click.option(
    "--port",
    type=int,
    default=os.getenv("FLIGHT_PORT", 8815),
    required=True,
    help="Port number to listen on"
)
@click.option(
    "--database-file",
    type=str,
    default=os.getenv("DATABASE_FILE", DUCKDB_DB_FILE.as_posix()),
    required=True,
    help="The DuckDB database file used for servicing data requests..."
)
@click.option(
    "--duckdb-threads",
    type=int,
    required=True,
    default=os.getenv("DUCKDB_THREADS", DUCKDB_THREADS),
    help="The number of threads to use for the DuckDB connection."
)
@click.option(
    "--duckdb-memory-limit",
    type=str,
    required=True,
    default=os.getenv("DUCKDB_MEMORY_LIMIT", DUCKDB_MEMORY_LIMIT),
    help="The amount of memory to use for the DuckDB connection"
)
@click.option(
    "--tls",
    nargs=2,
    default=os.getenv("FLIGHT_TLS").split(" ") if os.getenv("FLIGHT_TLS") else None,
    required=False,
    metavar=('CERTFILE', 'KEYFILE'),
    help="Enable transport-level security"
)
@click.option(
    "--verify-client/--no-verify-client",
    type=bool,
    default=(os.getenv("FLIGHT_VERIFY_CLIENT", "False").upper() == "TRUE"),
    show_default=True,
    required=True,
    help="enable mutual TLS and verify the client if True"
)
@click.option(
    "--mtls",
    type=str,
    default=os.getenv("FLIGHT_MTLS"),
    required=False,
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
    default=os.getenv("LOG_LEVEL", "INFO"),
    required=True,
    help="The logging level to use"
)
@click.option(
    "--log-file",
    type=str,
    default=os.getenv("LOG_FILE"),
    required=False,
    help="The log file to write to.  If None, will just log to stdout"
)
@click.option(
    "--log-file-mode",
    type=click.Choice(["a", "w"], case_sensitive=True),
    default=os.getenv("LOG_FILE_MODE", "w"),
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
    scheme = GRPC_TCP_SCHEME
    if tls:
        scheme = GRPC_TLS_SCHEME
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
    middleware = None
    if flight_username and flight_password:
        if not tls:
            raise RuntimeError("TLS must be enabled in order to use authentication, aborting.")
        auth_handler = NoOpAuthHandler()
        middleware = dict(basic=BasicAuthServerMiddlewareFactory(creds={flight_username: flight_password},
                                                                 cert=tls_cert_chain,
                                                                 key=tls_private_key,
                                                                 logger=logger
                                                                 ))

    host_uri = f"{scheme}://{host}:{port}"
    location_uri = f"{scheme}://{location}"
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
                          middleware=middleware,
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
        server.logger.warning(msg="Flight server shutdown")
        logging.shutdown()


if __name__ == '__main__':
    run_flight_server()
