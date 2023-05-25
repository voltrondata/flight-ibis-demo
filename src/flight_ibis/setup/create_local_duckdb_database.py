import click
import duckdb
from ..config import DUCKDB_DB_FILE, get_logger
from pathlib import Path


logger = get_logger()


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
@click.option(
    "--overwrite/--no-overwrite",
    type=bool,
    default=False,
    show_default=True,
    required=True,
    help="Can we overwrite the target --database-file if it already exists..."
)
def create_local_duckdb_database(database_file: str,
                                 scale_factor: float,
                                 overwrite: bool):
    logger.info(msg=f"create_local_duckdb_database - was called with args: {locals()}")

    logger.info(msg=f"Using DuckDB version: {duckdb.__version__}")

    database_file_path = Path(database_file)

    if database_file_path.exists():
        if overwrite:
            logger.warning(msg=f"Deleting existing database file: '{database_file_path.as_posix()}'")
            database_file_path.unlink(missing_ok=True)
        else:
            raise RuntimeError(f"Database file: '{database_file_path.as_posix()}' - already exists, aborting b/c overwrite False")

    # Get a DuckDB database connection
    with duckdb.connect(database=database_file_path.as_posix()) as conn:
        logger.info(msg=f"Creating DuckDB Database file: '{database_file_path.as_posix()}'")

        # Install the TPCH extension needed to generate the data...
        conn.install_extension(extension="tpch")
        conn.load_extension(extension="tpch")

        # Generate the data
        sql_statement = f"CALL dbgen(sf=?)"
        logger.info(f"Running SQL: {sql_statement} - with parameter: {scale_factor}")
        conn.execute(query=sql_statement,
                     parameters=[scale_factor]
                     )

        logger.info(msg=f"Successfully created DuckDB Database file: '{database_file_path.as_posix()}'")


if __name__ == '__main__':
    create_local_duckdb_database()
