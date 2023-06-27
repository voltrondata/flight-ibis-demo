from pyspark.sql import SparkSession
from pathlib import Path


def main():
    spark = (SparkSession
             .builder
             .appName("flight client")
             .config("spark.jars", "spark_connector/flight-spark-source-1.0-SNAPSHOT-shaded.jar")
             .getOrCreate())

    # Load the root CA if it is present in the tls directory
    root_ca_file = Path("tls/root_ca.crt").resolve()
    root_ca = None
    if root_ca_file.exists():
        with open(file=root_ca_file, mode="r") as file:
            root_ca = file.read()
    print(f"Root CA:\n{root_ca}")

    df = (spark.read.format('cdap.org.apache.arrow.flight.spark')
    .option('trustedCertificates', root_ca)
    .option('uri', 'grpc+tls://flight-ibis-nginx.vdfieldeng.com:443')
    .load(
        '{"command": "get_golden_rule_facts", "kwargs": {"num_threads": 1, "min_date": "1994-01-01T00:00:00", "max_date": "1995-12-31T00:00:00", "total_hash_buckets": 1, '
        '"hash_bucket_num": 13}'))

    df.show(n=10)


if __name__ == '__main__':
    main()
