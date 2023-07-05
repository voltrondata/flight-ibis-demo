from datetime import datetime


# Constants
LOCALHOST_IP_ADDRESS: str = "0.0.0.0"
LOCALHOST: str = "localhost"
GRPC_TCP_SCHEME: str = "grpc+tcp"  # No TLS enabled...
GRPC_TLS_SCHEME: str = "grpc+tls"
BEGINNING_OF_TIME: datetime = datetime(year=1994, month=1, day=1)
END_OF_TIME: datetime = datetime(year=9999, month=12, day=25)  # Merry last Christmas!
PYARROW_UNKNOWN: int = -1
JWT_ISS = "Flight Ibis"
JWT_AUD = "Flight Ibis"
