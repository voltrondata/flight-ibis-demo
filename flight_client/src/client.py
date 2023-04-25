import pyarrow as pa
import pyarrow.flight
from datetime import datetime
import json


def main():
    client = pa.flight.connect("grpc://0.0.0.0:8815")

    arg_dict = dict(num_threads=10,
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
        for chunk in reader:
            total_rows += chunk.data.num_rows
            # Print sample row
            print(chunk.data.take([0]).to_pandas())
    print("Got", total_rows, "rows total, expected", NUM_BATCHES * ROWS_PER_BATCH)


if __name__ == '__main__':
    main()
