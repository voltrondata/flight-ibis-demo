import click
import pyarrow as pa
import pyarrow.flight
import pyarrow.parquet
from config import get_file_logger
from data_logic import get_golden_rule_facts
import json
from munch import Munch, munchify
from datetime import datetime


# Constants
MAX_THREADS: int = 10
BEGINNING_OF_TIME: datetime = datetime(year=1775, month=11, day=10)
END_OF_TIME: datetime = datetime(year=9999, month=12, day=25)   # Merry last Christmas!

# Get a file based logger, b/c stdout logging doesn't work for a Python Flight server
logger = get_file_logger()


class FlightServer(pa.flight.FlightServerBase):
    @property
    def class_name(self):
        return self.__class__.__name__

    def __init__(self, host="localhost", location=None,
                 tls_certificates=None, verify_client=False,
                 root_certificates=None, auth_handler=None
                 ):
        super(FlightServer, self).__init__(
            location, auth_handler, tls_certificates, verify_client,
            root_certificates)
        self.flights = {}
        self.host = host
        self.tls_certificates = tls_certificates
        self._location = location
        print(f"Serving on {self._location}")

    def _make_flight_info(self, command):
        logger.debug(msg=f"{self.class_name}._make_flight_info - command: {command}")

        command_munch: Munch = munchify(x=json.loads(command))

        command_munch.kwargs.total_hash_buckets = min(MAX_THREADS, command_munch.kwargs.num_threads)

        descriptor = pa.flight.FlightDescriptor.for_command(
            command=command_munch.command.encode('utf-8')
        )
        logger.debug(msg=f"{self.class_name}._make_flight_info - descriptor: {descriptor}")

        endpoints = []
        for i in range(1, (command_munch.kwargs.total_hash_buckets + 1)):
            command_munch.kwargs.hash_bucket_num = i
            endpoints.append(pa.flight.FlightEndpoint(json.dumps(command_munch.toDict()), [self._location]))

        try:
            schema = get_golden_rule_facts(hash_bucket_num=99999,
                                           total_hash_buckets=1,
                                           min_date=BEGINNING_OF_TIME,
                                           max_date=BEGINNING_OF_TIME,
                                           schema_only=True
                                           ).to_pyarrow().schema
        except Exception as e:
            logger.error(msg=f"{self.class_name}._make_flight_info - Error: {str(e)}")
        else:
            return pyarrow.flight.FlightInfo(schema,
                                             descriptor,
                                             endpoints,
                                             0,
                                             0)

    def get_flight_info(self, context, descriptor):
        logger.debug(msg=f"{self.class_name}.get_flight_info - context={context}, descriptor={descriptor}")
        return self._make_flight_info(descriptor.command.decode('utf-8'))

    def do_get(self, context, ticket):
        logger.debug(msg=f"{self.class_name}.do_get - context = {context}, ticket = {ticket}")
        command_munch: Munch = munchify(x=json.loads(ticket.ticket.decode('utf-8')))

        command_kwargs = command_munch.kwargs

        if command_munch.command == "get_golden_rule_facts":
            try:
                reader = get_golden_rule_facts(hash_bucket_num=command_kwargs.hash_bucket_num,
                                               total_hash_buckets=command_kwargs.total_hash_buckets,
                                               min_date=datetime.fromisoformat(command_kwargs.min_date),
                                               max_date=datetime.fromisoformat(command_kwargs.max_date),
                                               schema_only=False
                                               ).to_pyarrow_batches()
            except Exception as e:
                error_message = f"{self.class_name}.get_flight_info - Error: {str(e)}"
                logger.error(msg=error_message)
                return pa.flight.FlightError(message=error_message)
            else:
                return pa.flight.GeneratorStream(schema=reader.schema,
                                                 generator=reader
                                                 )
        else:
            error_message = f"{self.class_name}.get_flight_info - Command: {command_munch.command} is not supported."
            logger.error(msg=error_message)
            return pa.flight.FlightError(message=error_message)


@click.command()
@click.option(
    "--host",
    type=str,
    default="0.0.0.0",
    help="Address or hostname to listen on"
)
@click.option(
    "--port",
    type=int,
    default=8815,
    help="Port number to listen on")
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
def main(host: str,
         port: int,
         tls: list,
         verify_client: bool
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
    server = FlightServer(host=host,
                          location=location,
                          tls_certificates=tls_certificates,
                          verify_client=verify_client)
    server.serve()


if __name__ == '__main__':
    main()
