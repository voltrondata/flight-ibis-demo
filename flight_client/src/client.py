import pyarrow as pa
import pyarrow.flight
from datetime import datetime
import json


def main():
    client = pa.flight.connect("grpc://0.0.0.0:8815")

    arg_dict = dict(num_threads=11,
                    min_date=datetime(year=1994, month=1, day=1).isoformat(),
                    max_date=datetime(year=1995, month=12, day=31).isoformat()
                    )
    command_dict = dict(command="get_golden_rule_facts",
                        kwargs=arg_dict
                        )
    upload_descriptor = pa.flight.FlightDescriptor.for_command(command=json.dumps(command_dict))

    # Read content of the dataset
    flight = client.get_flight_info(upload_descriptor)
    total_rows = 0
    for endpoint in flight.endpoints:
        reader = client.do_get(endpoint.ticket)
        read_table: pa.Table = reader.read_all()
        total_rows += read_table.num_rows
        print(read_table.to_pandas().head())
    print(f"Got {total_rows} rows total")


if __name__ == '__main__':
    main()
