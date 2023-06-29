import json
import os
from datetime import date, datetime
from pathlib import Path

import click
from codetiming import Timer
from pyspark.sql import SparkSession
from pyspark import __version__ as pyspark_version

from .config import logging, get_logger, TIMER_TEXT
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
    "--num-threads",
    type=int,
    default=1,
    required=True,
    help="The number of server threads to use for pulling data"
)
def run_spark_flight_client(host: str,
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

    with Timer(name=f"Spark Flight Client test",
               text=TIMER_TEXT,
               initial_text=True,
               logger=logger.info
               ):
        redacted_locals = {key: value for key, value in locals().items() if key not in ["flight_password"
                                                                                        ]
                           }
        logger.info(msg=f"run_spark_flight_client - was called with args: {redacted_locals}")

        logger.info(msg=f"Using PySpark version: {pyspark_version}")

        spark = (SparkSession
                 .builder
                 .appName("flight client")
                 .config("spark.jars", "spark_connector/flight-spark-source-1.0-SNAPSHOT-shaded.jar")
                 .getOrCreate())

        scheme = GRPC_TCP_SCHEME
        root_ca = ""
        if tls:
            scheme = GRPC_TLS_SCHEME
            # Load the root CA if it is present in the tls directory
            if tls_roots:
                root_ca_file = Path(tls_roots).resolve()
                if root_ca_file.exists():
                    with open(file=root_ca_file, mode="r") as file:
                        root_ca = file.read()
            logger.info(msg=f"Root CA:\n{root_ca}")

        mtls_cert_chain = ""
        mtls_private_key = ""
        if mtls:
            if not tls:
                raise RuntimeError("TLS must be enabled in order to use MTLS, aborting.")
            else:
                with open(file=mtls[0], mode="r") as cert_file:
                    mtls_cert_chain = cert_file.read()
                with open(file=mtls[1], mode="r") as key_file:
                    mtls_private_key = key_file.read()

        uri = f'{scheme}://{host}:{port}'
        logger.info(msg=f"Using Flight RPC Server URI: '{uri}'")

        arg_dict = dict(num_threads=num_threads,
                        min_date=from_date.isoformat(),
                        max_date=to_date.isoformat()
                        )
        command_dict = dict(command="get_golden_rule_facts",
                            kwargs=arg_dict
                            )
        logger.info(msg=f"Command dict: {command_dict}")

        df = (spark.read.format('cdap.org.apache.arrow.flight.spark')
        .option('trustedCertificates', root_ca)
        .option('uri', uri)
        .option('username', flight_username)
        .option('password', flight_password)
        .option('clientCertificate', mtls_cert_chain)
        .option('clientKey', mtls_private_key)
        .load(
            json.dumps(command_dict)
        )
        )

        df.show(n=10)


if __name__ == '__main__':
    run_spark_flight_client()
