import click
import pyarrow as pa
import pyarrow.flight
import pyarrow.parquet
import json
from munch import Munch, munchify
from datetime import datetime
from pathlib import Path
import ibis
import duckdb
from . import __version__ as flight_server_version
from .config import get_logger, logging, DUCKDB_DB_FILE, DUCKDB_THREADS, DUCKDB_MEMORY_LIMIT
from .data_logic_ibis import get_golden_rule_facts

# Constants
LOCALHOST: str = "0.0.0.0"
MAX_THREADS: int = 11
BEGINNING_OF_TIME: datetime = datetime(year=1775, month=11, day=10)
END_OF_TIME: datetime = datetime(year=9999, month=12, day=25)  # Merry last Christmas!
PYARROW_UNKNOWN: int = -1


class FlightServer(pa.flight.FlightServerBase):
    @property
    def class_name(self):
        return self.__class__.__name__

    def __init__(self,
                 location: str,
                 database_file: Path,
                 duckdb_threads: int,
                 duckdb_memory_limit: str,
                 tls_certificates=None,
                 verify_client=False,
                 root_certificates=None,
                 auth_handler=None,
                 log_file: str = None
                 ):
        self.logger = get_logger(filename=log_file,
                                 filemode="w",
                                 logger_name="flight_server"
                                 )

        self.logger.info(msg=f"Flight Server init args: {locals()}")

        if not database_file.exists():
            raise RuntimeError(f"The specified database file: '{database_file.as_posix()}' does not exist, aborting.")

        super(FlightServer, self).__init__(
            location=location,
            auth_handler=auth_handler,
            tls_certificates=tls_certificates,
            verify_client=verify_client,
            root_certificates=root_certificates
        )
        self.flights = {}
        self.tls_certificates = tls_certificates
        self._location = location

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
        self.logger.info(f"Serving on {self._location}")

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
            endpoints.append(pa.flight.FlightEndpoint(json.dumps(command_munch.toDict()), [self._location]))

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
            return pa.flight.FlightError(message=error_message)
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
            return pa.flight.FlightError(message=error_message)


@click.command()
@click.option(
    "--host",
    type=str,
    default=LOCALHOST,
    help="Address or hostname to listen on"
)
@click.option(
    "--port",
    type=int,
    default=8815,
    help="Port number to listen on")
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
def run_flight_server(host: str,
                      port: int,
                      database_file: str,
                      duckdb_threads: int,
                      duckdb_memory_limit: str,
                      tls: list,
                      verify_client: bool,
                      log_file: str
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

    location = f"{scheme}://{host}:{port}"
    server = FlightServer(location=location,
                          database_file=Path(database_file),
                          duckdb_threads=duckdb_threads,
                          duckdb_memory_limit=duckdb_memory_limit,
                          tls_certificates=tls_certificates,
                          verify_client=verify_client,
                          log_file=log_file
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
