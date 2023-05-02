import ibis
from ibis import _
from datetime import datetime
import pyarrow
from .config import DUCKDB_DB_FILE, DUCKDB_THREADS, DUCKDB_MEMORY_LIMIT, get_logger

# Constants
INNER_JOIN = "inner"
SEMI_JOIN = "semi"
MAX_ORDER_TOTALPRICE = 500_000.00
MAX_PERCENT_RANK = 0.98


# Ibis parameters (global in scope)
p_schema_only = ibis.param(type="Boolean")
p_min_date = ibis.param(type="date")
p_max_date = ibis.param(type="date")
p_total_hash_buckets = ibis.param(type="int")
p_hash_bucket_num = ibis.param(type="int")


def build_golden_rules_ibis_expression(conn: ibis.BaseBackend):
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
                    .filter(p_schema_only == False)
                    )

    # Filter orders to the hash bucket asked for
    # Filter out orders larger than MAX_ORDER_TOTALPRICE
    orders_prelim = (orders
                     .alias("orders_sql")
                     .sql("SELECT orders_sql.*, hash(orders_sql.o_orderkey) AS hash_result FROM orders_sql")
                     .filter(_.o_orderdate.between(lower=p_min_date, upper=p_max_date))
                     .mutate(hash_bucket=(_.hash_result % p_total_hash_buckets))
                     .filter(_.hash_bucket == (p_hash_bucket_num - 1))
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
    golden_rule_facts_expr = (orders_filtered.join(right=lineitem_filtered,
                                                   predicates=orders_filtered.o_orderkey == lineitem_filtered.l_orderkey,
                                                   how=INNER_JOIN
                                                   )
                              )

    return golden_rule_facts_expr


def get_golden_rule_facts(golden_rules_ibis_expression: ibis.Expr,
                          hash_bucket_num: int,
                          total_hash_buckets: int,
                          min_date: datetime,
                          max_date: datetime,
                          schema_only: bool = False,
                          existing_logger=None,
                          log_file: str = None
                          ) -> pyarrow.Table:
    try:
        if existing_logger:
            logger = existing_logger
        else:
            logger = get_logger(filename=log_file,
                                filemode="w",
                                logger_name="data_logic"
                                )

        logger.debug(f"get_golden_rule_facts - was called with args: {locals()}")

        pyarrow_dataset = (golden_rules_ibis_expression
                           .to_pyarrow_batches(params={p_hash_bucket_num: hash_bucket_num,
                                                       p_total_hash_buckets: total_hash_buckets,
                                                       p_min_date: min_date,
                                                       p_max_date: max_date,
                                                       p_schema_only: schema_only
                                                       }
                                               )
                           )

        logger.debug(f"get_golden_rule_facts - successfully converted Ibis expression to PyArrow.")

    except Exception as e:
        logger.exception(msg=f"get_golden_rule_facts - Exception: {str(e)}")
        raise
    else:
        return pyarrow_dataset
    finally:
        logger.debug(msg=f"get_golden_rule_facts - Finally block")
        if not existing_logger:
            logger.handlers.clear()


if __name__ == '__main__':
    logger = get_logger()
    TOTAL_HASH_BUCKETS: int = 11

    conn = ibis.duckdb.connect(database=DUCKDB_DB_FILE,
                               threads=DUCKDB_THREADS,
                               memory_limit=DUCKDB_MEMORY_LIMIT,
                               read_only=True
                               )
    golden_rules_ibis_expression = build_golden_rules_ibis_expression(conn=conn)
    for i in range(1, TOTAL_HASH_BUCKETS + 1):
        logger.info(msg=f"Bucket #: {i}")
        reader = get_golden_rule_facts(golden_rules_ibis_expression=golden_rules_ibis_expression,
                                       hash_bucket_num=i,
                                       total_hash_buckets=TOTAL_HASH_BUCKETS,
                                       min_date=datetime(year=1994, month=1, day=1),
                                       max_date=datetime(year=1997, month=12, day=31),
                                       schema_only=False,
                                       existing_logger=logger
                                       )
        first_chunk_for_batch = True
        for chunk in reader:
            if first_chunk_for_batch:
                logger.info(msg=chunk.to_pandas().head(n=10))
            first_chunk_for_batch = False
