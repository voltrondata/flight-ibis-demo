import click
import duckdb
from config import DUCKDB_DB_FILE, get_stdout_logger

logger = get_stdout_logger()


@click.command()
@click.option(
    "--database-file",
    type=str,
    default=DUCKDB_DB_FILE.as_posix(),
    help="The database file to create"
)
@click.option(
    "--scale-factor",
    type=float,
    default=1.0,
    help="TPC-H Scale Factor used to create the database file."
)
def main(database_file: str,
         scale_factor: float):
    # Delete the database if it exists...
    DUCKDB_DB_FILE.unlink(missing_ok=True)

    # Get a DuckDB database connection
    with duckdb.connect(database=DUCKDB_DB_FILE.as_posix()) as conn:
        # Install the TPCH extension needed to generate the data...
        conn.install_extension(extension="tpch")
        conn.load_extension(extension="tpch")

        # Generate the data
        conn.execute(query=f"CALL dbgen(sf={scale_factor})")

        logger.info(msg=f"Successfully created DuckDB Database file: {DUCKDB_DB_FILE.as_posix()}")


if __name__ == '__main__':
    main()
