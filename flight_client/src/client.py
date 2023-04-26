import pyarrow as pa
import pyarrow.flight
from datetime import datetime
import json
import click
from config import get_logger


# Constants
LOCALHOST: str = "0.0.0.0"


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
    help="Port number of the Flight server to connect to")
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
    help="Enable transport-level security")
@click.option(
    "--verify_client",
    type=bool,
    default=False,
    help="enable mutual TLS and verify the client if True")
@click.option(
    "--log-file",
    type=str,
    default=None,
    help="The log file to write to.  If None, will just log to stdout"
)
def run_flight_client(host: str,
                      port: int,
                      tls: bool,
                      tls_roots: str,
                      mtls: list,
                      verify_client: bool,
                      log_file: str
                      ):
    logger = get_logger(filename=log_file,
                        filemode="w",
                        logger_name="flight_client"
                        )

    logger.info(msg=f"run_flight_client - was called with args: {locals()}")

    scheme = "grpc+tcp"
    connection_args = {}
    if tls:
        scheme = "grpc+tls"
        if tls_roots:
            with open(tls_roots, "rb") as root_certs:
                connection_args["tls_root_certs"] = root_certs.read()
    if mtls:
        with open(mtls[0], "rb") as cert_file:
            tls_cert_chain = cert_file.read()
        with open(mtls[1], "rb") as key_file:
            tls_private_key = key_file.read()
        connection_args["cert_chain"] = tls_cert_chain
        connection_args["private_key"] = tls_private_key
    client = pyarrow.flight.FlightClient(f"{scheme}://{host}:{port}",
                                         **connection_args)

    arg_dict = dict(num_threads=11,
                    min_date=datetime(year=1994, month=1, day=1).isoformat(),
                    max_date=datetime(year=1995, month=12, day=31).isoformat()
                    )
    command_dict = dict(command="get_golden_rule_facts",
                        kwargs=arg_dict
                        )
    command_descriptor = pa.flight.FlightDescriptor.for_command(command=json.dumps(command_dict))

    # Read content of the dataset
    flight = client.get_flight_info(command_descriptor)
    total_rows = 0
    for endpoint in flight.endpoints:
        reader = client.do_get(endpoint.ticket)
        read_table: pa.Table = reader.read_all()
        total_rows += read_table.num_rows
        logger.info(msg=read_table.to_pandas().head())
    logger.info(msg=f"Got {total_rows} rows total")


if __name__ == '__main__':
    run_flight_client()
