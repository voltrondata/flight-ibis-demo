import ibis
from ibis import _
from config import DUCKDB_DB_FILE, get_logger
from datetime import datetime


# Constants
INNER_JOIN = "inner"
SEMI_JOIN = "semi"
MAX_ORDER_TOTALPRICE = 500_000.00
MAX_PERCENT_RANK = 0.98
DUCKDB_THREADS: int = 4
DUCKDB_MEMORY_LIMIT: str = "4GB"


def get_golden_rule_facts(hash_bucket_num: int,
                          total_hash_buckets: int,
                          min_date: datetime,
                          max_date: datetime,
                          schema_only: bool = False,
                          existing_logger = None
                          ) -> ibis.Expr:
    try:
        if existing_logger:
            logger = existing_logger
        else:
            logger = get_logger(filename=f"data_logic_{hash_bucket_num}.log",
                                filemode="w",
                                logger_name="data_logic"
                                )

        logger.debug(f"get_golden_rule_facts - was called with args: {locals()}")

        conn = ibis.duckdb.connect(database=DUCKDB_DB_FILE,
                                   threads=DUCKDB_THREADS,
                                   memory_limit=DUCKDB_MEMORY_LIMIT,
                                   read_only=True
                                   )

        logger.debug(f"get_golden_rule_facts - successfully got ibis DuckDB connection.")

        orders = conn.table("orders")
        lineitem = conn.table("lineitem")
        region = conn.table("region")
        nation = conn.table("nation")
        customer = conn.table("customer")
        part = conn.table("part")

        # Aggregate global order counts for use in a subsequent filter
        order_counts = (orders
                        .group_by(_.o_custkey)
                        .aggregate(count_star=_.count())
                        .mutate(order_count_percent_rank=_.count_star.percent_rank()
                                .over(ibis.window(order_by=_.count_star))
                                )
                        .filter(_.order_count_percent_rank <= MAX_PERCENT_RANK)
                        .filter(ibis.literal(schema_only) == False)
                        )

        # Filter orders to the hash bucket asked for
        # Filter out orders larger than MAX_ORDER_TOTALPRICE
        orders_prelim = (orders
                         .filter(_.o_orderdate.between(lower=min_date, upper=max_date))
                         .alias("orders_sql")
                         .sql("SELECT orders_sql.*, hash(orders_sql.o_orderkey) AS hash_result FROM orders_sql")
                         .mutate(hash_bucket=(_.hash_result % total_hash_buckets))
                         .filter(_.hash_bucket == (hash_bucket_num - 1))
                         .filter(_.o_totalprice <= MAX_ORDER_TOTALPRICE)
                         .drop("o_comment", "hash_result", "hash_bucket")
                         )

        # Filter out orders with customers that have more orders than the MAX_PERCENT_RANK
        # This simulates filtering out store cards
        orders_pre_filtered = orders_prelim.filter((orders_prelim.o_custkey == order_counts.o_custkey).any())

        # Filter out the European region
        region_filtered = (region
                           .filter(_.r_name.notin(["EUROPE"])
                                   )
                           )

        nation_filtered = (nation
                           .filter((nation.n_regionkey == region_filtered.r_regionkey).any())
                           )

        customer_filtered = (customer
                             .filter((customer.c_nationkey == nation_filtered.n_nationkey).any())
                             )

        orders_filtered = (orders_pre_filtered
                           .filter((orders_pre_filtered.o_custkey == customer_filtered.c_custkey).any())
                           )

        # Filter out Manufacturer#3 from parts
        part_filtered = (part
                         .filter(part.p_mfgr.notin(["Manufacturer#3"]
                                                   )
                                 )
                         )

        lineitem_filtered = (lineitem
                             .filter((lineitem.l_partkey == part_filtered.p_partkey).any())
                             )

        # Join lineitem to orders - keep the columns as well...
        golden_rule_facts = (orders_filtered.join(right=lineitem_filtered,
                                                  predicates=orders_filtered.o_orderkey == lineitem_filtered.l_orderkey,
                                                  how=INNER_JOIN
                                                  )
                             )

        logger.debug(f"get_golden_rule_facts - successfully built Ibis expression.")

    except Exception as e:
        logger.exception(msg=f"get_golden_rule_facts - Exception: {str(e)}")
        raise
    else:
        logger.debug(f"get_golden_rule_facts - trying to convert to PyArrow.")
        pyarrow_dataset = golden_rule_facts.to_pyarrow()
        logger.debug(f"get_golden_rule_facts - successfully converted to PyArrow.")

        return pyarrow_dataset
    finally:
        logger.debug(msg=f"get_golden_rule_facts - Finally")
        if not existing_logger:
            logger.handlers.clear()


if __name__ == '__main__':
    logger = get_logger()
    TOTAL_HASH_BUCKETS: int = 11
    for i in range(1, TOTAL_HASH_BUCKETS + 1):
        logger.info(msg=f"Bucket #: {i}")
        x = get_golden_rule_facts(hash_bucket_num=i,
                                  total_hash_buckets=TOTAL_HASH_BUCKETS,
                                  min_date=datetime(year=1994, month=1, day=1),
                                  max_date=datetime(year=1997, month=12, day=31),
                                  schema_only=False,
                                  existing_logger=logger
                                  )
        logger.info(msg=x.to_pandas().head(n=10))
