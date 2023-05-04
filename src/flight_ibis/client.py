import pyarrow as pa
import pyarrow.flight
from datetime import date, datetime
import json
import click
import logging
import os
from codetiming import Timer
from .config import TIMER_TEXT, get_logger


# Constants
LOCALHOST: str = "0.0.0.0"


class TokenClientAuthHandler(pyarrow.flight.ClientAuthHandler):
    """An example implementation of authentication via handshake."""

    def __init__(self, username, password):
        super().__init__()
        self.username = username
        self.password = password
        self.token = b''

    def authenticate(self, outgoing, incoming):
        outgoing.write(self.username)
        outgoing.write(self.password)
        self.token = incoming.read()

    def get_token(self):
        return self.token


@click.command()
@click.option(
    "--host",
    type=str,
    default=LOCALHOST,
    help="Address or hostname of the Flight server to connect to"
)
@click.option(
    "--port",
    type=int,
    default=8815,
    help="Port number of the Flight server to connect to"
)
@click.option(
    "--tls/--no-tls",
    type=bool,
    default=False,
    show_default=True,
    required=True,
    help="Connect to the server with tls"
)
@click.option(
    "--tls-roots",
    type=str,
    default=None,
    show_default=True,
    help="'Path to trusted TLS certificate(s)"
)
@click.option(
    "--mtls",
    nargs=2,
    default=None,
    metavar=('CERTFILE', 'KEYFILE'),
    help="Enable transport-level security"
)
@click.option(
    "--flight-username",
    type=str,
    default=os.getenv("FLIGHT_USERNAME"),
    required=False,
    show_default=False,
    help="The username used to connect to the Flight server"
)
@click.option(
    "--flight-password",
    type=str,
    default=os.getenv("FLIGHT_PASSWORD"),
    required=False,
    show_default=False,
    help="The password used to connect to the Flight server"
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
    required=False,
    help="The log file to write to.  If None, will just log to stdout"
)
@click.option(
    "--log-file-mode",
    type=click.Choice(["a", "w"], case_sensitive=True),
    default="w",
    help="The log file mode, use value: a for 'append', and value: w to overwrite..."
)
@click.option(
    "--from-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=date(year=1994, month=1, day=1).isoformat(),
    required=True,
    help="The from date to use for the data filter - in ISO format (example: 2020-11-01) - for 01-November-2020"
)
@click.option(
    "--to-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=date(year=1995, month=12, day=31).isoformat(),
    required=True,
    help="The to date to use for the data filter - in ISO format (example: 2020-11-01) - for 01-November-2020"
)
@click.option(
    "--num-threads",
    type=int,
    default=11,
    required=True,
    help="The number of server threads to use for pulling data"
)
def run_flight_client(host: str,
                      port: int,
                      tls: bool,
                      tls_roots: str,
                      mtls: list,
                      flight_username: str,
                      flight_password: str,
                      log_level: str,
                      log_file: str,
                      log_file_mode: str,
                      from_date: datetime,
                      to_date: datetime,
                      num_threads: int
                      ):
    logger = get_logger(filename=log_file,
                        filemode=log_file_mode,
                        logger_name="flight_client",
                        log_level=getattr(logging, log_level.upper())
                        )

    with Timer(name=f"Flight Client test",
               text=TIMER_TEXT,
               initial_text=True,
               logger=logger.info
               ):
        redacted_locals = {key: value for key, value in locals().items() if key not in ["flight_password"
                                                                                        ]
                           }
        logger.info(msg=f"run_flight_client - was called with args: {redacted_locals}")

        scheme = "grpc+tcp"
        connection_args = {}
        if tls:
            scheme = "grpc+tls"
            if tls_roots:
                with open(tls_roots, "rb") as root_certs:
                    connection_args["tls_root_certs"] = root_certs.read()
        if mtls:
            if not tls:
                raise RuntimeError("TLS must be enabled in order to use MTLS, aborting.")
            else:
                with open(mtls[0], "rb") as cert_file:
                    tls_cert_chain = cert_file.read()
                with open(mtls[1], "rb") as key_file:
                    tls_private_key = key_file.read()
                connection_args["cert_chain"] = tls_cert_chain
                connection_args["private_key"] = tls_private_key

        client = pyarrow.flight.FlightClient(f"{scheme}://{host}:{port}",
                                             **connection_args)

        if flight_username and flight_password:
            if not tls:
                raise RuntimeError("TLS must be enabled in order to use authentication, aborting.")

            client.authenticate(TokenClientAuthHandler(username=flight_username,
                                                       password=flight_password
                                                       )
                                )

        # Display session authentication info (if applicable)
        if flight_username or mtls:
            action = pyarrow.flight.Action("who-am-i", b"")
            who_am_i_results = list(client.do_action(action=action))[0]
            authenticated_user = who_am_i_results.body.to_pybytes().decode()
            if authenticated_user:
                logger.info(f"Authenticated to the Flight Server as user: {authenticated_user}")

        arg_dict = dict(num_threads=num_threads,
                        min_date=from_date.isoformat(),
                        max_date=to_date.isoformat()
                        )
        command_dict = dict(command="get_golden_rule_facts",
                            kwargs=arg_dict
                            )
        command_descriptor = pa.flight.FlightDescriptor.for_command(command=json.dumps(command_dict))

        # Read content of the dataset
        flight = client.get_flight_info(command_descriptor)
        total_endpoints = 0
        total_chunks = 0
        total_rows = 0
        total_bytes = 0

        for endpoint in flight.endpoints:
            with Timer(name=f"Fetch data from Flight Server end-point: {endpoint}",
                       text=TIMER_TEXT,
                       initial_text=True,
                       logger=logger.debug
                       ):
                total_endpoints += 1
                reader = client.do_get(endpoint.ticket)
                first_chunk_for_endpoint = True
                for chunk in reader:
                    total_chunks += 1
                    data = chunk.data
                    logger.debug(msg=f"Chunk size - rows: {data.num_rows} / bytes: {data.nbytes}")
                    if first_chunk_for_endpoint:
                        logger.info(msg=data.to_pandas().head())
                    total_rows += data.num_rows
                    total_bytes += data.nbytes
                    first_chunk_for_endpoint = False

        logger.info(msg=f"Got {total_rows} rows total ({total_bytes} bytes) - from {total_endpoints} endpoints ({total_chunks} total chunks)")


if __name__ == '__main__':
    run_flight_client()
