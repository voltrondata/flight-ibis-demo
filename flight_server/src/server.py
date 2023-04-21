import click
import pathlib
import pyarrow as pa
import pyarrow.flight
import pyarrow.parquet
from config import get_file_logger
from data_logic import apply_golden_rules


# Get a file based logger, b/c stdout logging doesn't work for a Python Flight server
logger = get_file_logger()


class FlightServer(pa.flight.FlightServerBase):
    def __init__(self, host="localhost", location=None,
                 tls_certificates=None, verify_client=False,
                 root_certificates=None, auth_handler=None,
                 repo=pathlib.Path("./datasets")
                 ):
        super(FlightServer, self).__init__(
            location, auth_handler, tls_certificates, verify_client,
            root_certificates)
        self.flights = {}
        self.host = host
        self.tls_certificates = tls_certificates
        self._location = location
        self._repo = repo
        print(f"Serving on {self._location}")

    def _make_flight_info(self, dataset):
        dataset_path = self._repo / dataset
        schema = pa.parquet.read_schema(dataset_path)
        metadata = pa.parquet.read_metadata(dataset_path)
        descriptor = pa.flight.FlightDescriptor.for_path(
            dataset.encode('utf-8')
        )
        logger.info(msg=f"Descriptor: {descriptor}")
        endpoints = [pa.flight.FlightEndpoint(dataset, [self._location])]
        return pyarrow.flight.FlightInfo(schema,
                                        descriptor,
                                        endpoints,
                                        metadata.num_rows,
                                        metadata.serialized_size)

    def list_flights(self, context, criteria):
        for dataset in self._repo.iterdir():
            yield self._make_flight_info(dataset.name)

    def get_flight_info(self, context, descriptor):
        return self._make_flight_info(descriptor.path[0].decode('utf-8'))

    def do_put(self, context, descriptor, reader, writer):
        dataset = descriptor.path[0].decode('utf-8')
        dataset_path = self._repo / dataset
        # Read the uploaded data and write to Parquet incrementally
        with dataset_path.open("wb") as sink:
            with pa.parquet.ParquetWriter(sink, reader.schema) as writer:
                for chunk in reader:
                    writer.write_table(pa.Table.from_batches([chunk.data]))

    def do_get(self, context, ticket):
        logger.info(msg=f"ticket = {ticket}")
        dataset = ticket.ticket.decode('utf-8')
        # # Stream data from a file
        # dataset_path = self._repo / dataset
        # reader = pa.parquet.ParquetFile(dataset_path)
        # return pa.flight.GeneratorStream(
        #     reader.schema_arrow, reader.iter_batches())
        reader = apply_golden_rules().to_pyarrow_batches()
        return pa.flight.GeneratorStream(schema=reader.schema,
                                         generator=reader
                                         )

    def list_actions(self, context):
        return [
            ("drop_dataset", "Delete a dataset."),
        ]

    def do_action(self, context, action):
        if action.type == "drop_dataset":
            self.do_drop_dataset(action.body.to_pybytes().decode('utf-8'))
        else:
            raise NotImplementedError

    def do_drop_dataset(self, dataset):
        dataset_path = self._repo / dataset
        dataset_path.unlink()

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
    server._repo.mkdir(exist_ok=True)
    server.serve()


if __name__ == '__main__':
    main()
