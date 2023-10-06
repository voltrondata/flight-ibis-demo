import pyarrow as pa
import pyarrow.flight
from datetime import date, datetime
import json
import click
import logging
import os
from codetiming import Timer
from .config import TIMER_TEXT, get_logger
from .constants import LOCALHOST, GRPC_TCP_SCHEME, GRPC_TLS_SCHEME


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
    "--num-endpoints",
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
                      num_endpoints: int
                      ):
    logger = get_logger(filename=log_file,
                        filemode=log_file_mode,
                        logger_name="flight_client",
                        log_level=getattr(logging, log_level.upper())
                        )

    with Timer(name="Flight Client test",
               text=TIMER_TEXT,
               initial_text=True,
               logger=logger.info
               ):
        redacted_locals = {key: value for key, value in locals().items() if key not in ["flight_password"
                                                                                        ]
                           }
        logger.info(msg=f"run_flight_client - was called with args: {redacted_locals}")

        scheme = GRPC_TCP_SCHEME
        connection_args = {}
        if tls:
            scheme = GRPC_TLS_SCHEME
            if tls_roots:
                with open(tls_roots, "rb") as root_certs:
                    connection_args["tls_root_certs"] = root_certs.read()
        if mtls:
            if not tls:
                raise RuntimeError("TLS must be enabled in order to use MTLS, aborting.")
            else:
                with open(mtls[0], "rb") as cert_file:
                    mtls_cert_chain = cert_file.read()
                with open(mtls[1], "rb") as key_file:
                    mtls_private_key = key_file.read()
                connection_args["cert_chain"] = mtls_cert_chain
                connection_args["private_key"] = mtls_private_key

        flight_server_uri = f"{scheme}://{host}:{port}"
        client = pyarrow.flight.FlightClient(location=flight_server_uri,
                                             **connection_args)

        logger.info(msg=f"Connected to Flight Server at location: {flight_server_uri}")

        options = None
        if flight_username and flight_password:
            if not tls:
                raise RuntimeError("TLS must be enabled in order to use authentication, aborting.")
            token_pair = client.authenticate_basic_token(username=flight_username.encode(),
                                                         password=flight_password.encode(),
                                                         )
            logger.debug(f"token_pair = {token_pair}")
            options = pa.flight.FlightCallOptions(headers=[token_pair])

        # Display session authentication info (if applicable)
        if flight_username or mtls:
            action = pyarrow.flight.Action("who-am-i", b"")
            who_am_i_results = list(client.do_action(action=action, options=options))[0]
            authenticated_user = who_am_i_results.body.to_pybytes().decode()
            if authenticated_user:
                logger.info(f"Authenticated to the Flight Server as user: {authenticated_user}")

        arg_dict = dict(num_endpoints=num_endpoints,
                        min_date=from_date.isoformat(),
                        max_date=to_date.isoformat()
                        )
        command_dict = dict(command="get_golden_rule_facts",
                            kwargs=arg_dict,
                            columns=["o_orderkey", "o_custkey"],
                            filters=[{"column": "o_custkey",
                                      "operator": "=",
                                      "value": 121717
                                      }
                                     ]
                            )
        command_descriptor = pa.flight.FlightDescriptor.for_command(command=json.dumps(command_dict))
        logger.info(msg=f"Command Descriptor: {command_descriptor}")
        # Read content of the dataset
        flight = client.get_flight_info(command_descriptor, options)

        logger.debug(msg=f"Flight Schema: {flight.schema}")
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
                reader = client.do_get(endpoint.ticket, options)
                for chunk in reader:
                    total_chunks += 1
                    data = chunk.data
                    if data:
                        logger.debug(msg=f"Chunk size - rows: {data.num_rows} / bytes: {data.nbytes}")
                        logger.debug(msg=f"Endpoint: {endpoint} / Chunk: {total_chunks}")
                        logger.info(msg=data.to_pandas().head())
                        total_rows += data.num_rows
                        total_bytes += data.nbytes

        logger.info(msg=f"Got {total_rows} rows total ({total_bytes} bytes) - from {total_endpoints} endpoints ({total_chunks} total chunks)")


if __name__ == '__main__':
    run_flight_client()
