import duckdb
import pyarrow
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
                          existing_logger=None
                          ) -> pyarrow.Table:
    try:
        if existing_logger:
            logger = existing_logger
        else:
            logger = get_logger(filename=f"data_logic_{hash_bucket_num}.log",
                                filemode="w",
                                logger_name="data_logic"
                                )

        logger.debug(f"get_golden_rule_facts - was called with args: {locals()}")

        with duckdb.connect(database=DUCKDB_DB_FILE.as_posix(),
                            read_only=True,
                            config=dict(threads=DUCKDB_THREADS,
                                        memory_limit=DUCKDB_MEMORY_LIMIT
                                        )
                            ) as conn:
            logger.debug(f"get_golden_rule_facts - successfully got DuckDB connection.")

            # Got the generated SQL from Ibis
            sql_statement = f"""WITH _ibis_view_10 AS (
                                  SELECT
                                    orders.*,
                                    HASH(orders.o_orderkey) AS hash_result
                                  FROM orders
                                  WHERE orders.o_orderdate BETWEEN ? AND ?
                                ), t1 AS (
                                  SELECT
                                    t13.o_orderkey AS o_orderkey,
                                    t13.o_custkey AS o_custkey,
                                    t13.o_orderstatus AS o_orderstatus,
                                    t13.o_totalprice AS o_totalprice,
                                    t13.o_orderdate AS o_orderdate,
                                    t13.o_orderpriority AS o_orderpriority,
                                    t13.o_clerk AS o_clerk,
                                    t13.o_shippriority AS o_shippriority,
                                    t13.o_comment AS o_comment,
                                    t13.hash_result AS hash_result,
                                    t13.hash_result % CAST(? AS SMALLINT) AS hash_bucket
                                  FROM _ibis_view_10 AS t13
                                ), t4 AS (
                                  SELECT
                                    t1.o_orderkey AS o_orderkey,
                                    t1.o_custkey AS o_custkey,
                                    t1.o_orderstatus AS o_orderstatus,
                                    t1.o_totalprice AS o_totalprice,
                                    t1.o_orderdate AS o_orderdate,
                                    t1.o_orderpriority AS o_orderpriority,
                                    t1.o_clerk AS o_clerk,
                                    t1.o_shippriority AS o_shippriority,
                                    t1.o_comment AS o_comment,
                                    t1.hash_result AS hash_result,
                                    t1.hash_bucket AS hash_bucket
                                  FROM t1
                                  WHERE
                                    t1.hash_bucket = CAST(? AS SMALLINT) AND t1.o_totalprice <= CAST(? AS DOUBLE)
                                ), t7 AS (
                                  SELECT
                                    t4.o_orderkey AS o_orderkey,
                                    t4.o_custkey AS o_custkey,
                                    t4.o_orderstatus AS o_orderstatus,
                                    t4.o_totalprice AS o_totalprice,
                                    t4.o_orderdate AS o_orderdate,
                                    t4.o_orderpriority AS o_orderpriority,
                                    t4.o_clerk AS o_clerk,
                                    t4.o_shippriority AS o_shippriority
                                  FROM t4
                                ), t0 AS (
                                  SELECT
                                    t13.o_custkey AS o_custkey,
                                    COUNT(*) AS count_star
                                  FROM orders AS t13
                                  GROUP BY
                                    1
                                ), t3 AS (
                                  SELECT
                                    t0.o_custkey AS o_custkey,
                                    t0.count_star AS count_star,
                                    PERCENT_RANK() OVER (ORDER BY t0.count_star) AS order_count_percent_rank
                                  FROM t0
                                ), t6 AS (
                                  SELECT
                                    t3.o_custkey AS o_custkey,
                                    t3.count_star AS count_star,
                                    t3.order_count_percent_rank AS order_count_percent_rank
                                  FROM t3
                                  WHERE
                                    t3.order_count_percent_rank <= CAST(? AS DOUBLE)
                                    AND CAST(FALSE AS BOOLEAN) = CAST(FALSE AS BOOLEAN)
                                ), t10 AS (
                                  SELECT
                                    t7.o_orderkey AS o_orderkey,
                                    t7.o_custkey AS o_custkey,
                                    t7.o_orderstatus AS o_orderstatus,
                                    t7.o_totalprice AS o_totalprice,
                                    t7.o_orderdate AS o_orderdate,
                                    t7.o_orderpriority AS o_orderpriority,
                                    t7.o_clerk AS o_clerk,
                                    t7.o_shippriority AS o_shippriority
                                  FROM t7
                                  WHERE
                                    EXISTS(
                                      SELECT
                                        CAST(1 AS SMALLINT) AS anon_1
                                      FROM t6
                                      WHERE
                                        t7.o_custkey = t6.o_custkey
                                    )
                                ), t2 AS (
                                  SELECT
                                    t13.r_regionkey AS r_regionkey,
                                    t13.r_name AS r_name,
                                    t13.r_comment AS r_comment
                                  FROM region AS t13
                                  WHERE
                                    (
                                      NOT t13.r_name IN ('EUROPE')
                                    )
                                ), t5 AS (
                                  SELECT
                                    t13.n_nationkey AS n_nationkey,
                                    t13.n_name AS n_name,
                                    t13.n_regionkey AS n_regionkey,
                                    t13.n_comment AS n_comment
                                  FROM nation AS t13
                                  WHERE
                                    EXISTS(
                                      SELECT
                                        CAST(1 AS SMALLINT) AS anon_4
                                      FROM t2
                                      WHERE
                                        t13.n_regionkey = t2.r_regionkey
                                    )
                                ), t9 AS (
                                  SELECT
                                    t13.c_custkey AS c_custkey,
                                    t13.c_name AS c_name,
                                    t13.c_address AS c_address,
                                    t13.c_nationkey AS c_nationkey,
                                    t13.c_phone AS c_phone,
                                    t13.c_acctbal AS c_acctbal,
                                    t13.c_mktsegment AS c_mktsegment,
                                    t13.c_comment AS c_comment
                                  FROM customer AS t13
                                  WHERE
                                    EXISTS(
                                      SELECT
                                        CAST(1 AS SMALLINT) AS anon_3
                                      FROM t5
                                      WHERE
                                        t13.c_nationkey = t5.n_nationkey
                                    )
                                ), t12 AS (
                                  SELECT
                                    t10.o_orderkey AS o_orderkey,
                                    t10.o_custkey AS o_custkey,
                                    t10.o_orderstatus AS o_orderstatus,
                                    t10.o_totalprice AS o_totalprice,
                                    t10.o_orderdate AS o_orderdate,
                                    t10.o_orderpriority AS o_orderpriority,
                                    t10.o_clerk AS o_clerk,
                                    t10.o_shippriority AS o_shippriority
                                  FROM t10
                                  WHERE
                                    EXISTS(
                                      SELECT
                                        CAST(1 AS SMALLINT) AS anon_2
                                      FROM t9
                                      WHERE
                                        t10.o_custkey = t9.c_custkey
                                    )
                                ), t8 AS (
                                  SELECT
                                    t13.p_partkey AS p_partkey,
                                    t13.p_name AS p_name,
                                    t13.p_mfgr AS p_mfgr,
                                    t13.p_brand AS p_brand,
                                    t13.p_type AS p_type,
                                    t13.p_size AS p_size,
                                    t13.p_container AS p_container,
                                    t13.p_retailprice AS p_retailprice,
                                    t13.p_comment AS p_comment
                                  FROM part AS t13
                                  WHERE
                                    (
                                      NOT t13.p_mfgr IN ('Manufacturer#3')
                                    )
                                ), t11 AS (
                                  SELECT
                                    t13.l_orderkey AS l_orderkey,
                                    t13.l_partkey AS l_partkey,
                                    t13.l_suppkey AS l_suppkey,
                                    t13.l_linenumber AS l_linenumber,
                                    t13.l_quantity AS l_quantity,
                                    t13.l_extendedprice AS l_extendedprice,
                                    t13.l_discount AS l_discount,
                                    t13.l_tax AS l_tax,
                                    t13.l_returnflag AS l_returnflag,
                                    t13.l_linestatus AS l_linestatus,
                                    t13.l_shipdate AS l_shipdate,
                                    t13.l_commitdate AS l_commitdate,
                                    t13.l_receiptdate AS l_receiptdate,
                                    t13.l_shipinstruct AS l_shipinstruct,
                                    t13.l_shipmode AS l_shipmode,
                                    t13.l_comment AS l_comment
                                  FROM lineitem AS t13
                                  WHERE
                                    EXISTS(
                                      SELECT
                                        CAST(1 AS SMALLINT) AS anon_5
                                      FROM t8
                                      WHERE
                                        t13.l_partkey = t8.p_partkey
                                    )
                                )
                                SELECT
                                  t12.o_orderkey,
                                  t12.o_custkey,
                                  t12.o_orderstatus,
                                  t12.o_totalprice,
                                  t12.o_orderdate,
                                  t12.o_orderpriority,
                                  t12.o_clerk,
                                  t12.o_shippriority,
                                  t11.l_orderkey,
                                  t11.l_partkey,
                                  t11.l_suppkey,
                                  t11.l_linenumber,
                                  t11.l_quantity,
                                  t11.l_extendedprice,
                                  t11.l_discount,
                                  t11.l_tax,
                                  t11.l_returnflag,
                                  t11.l_linestatus,
                                  t11.l_shipdate,
                                  t11.l_commitdate,
                                  t11.l_receiptdate,
                                  t11.l_shipinstruct,
                                  t11.l_shipmode,
                                  t11.l_comment
                                FROM t12
                                JOIN t11
                                  ON t12.o_orderkey = t11.l_orderkey"""

            logger.debug(f"get_golden_rule_facts - Running SQL:\n{sql_statement}")

            pyarrow_dataset = conn.execute(query=sql_statement,
                                           parameters=[min_date,
                                                       max_date,
                                                       total_hash_buckets,
                                                       (hash_bucket_num - 1),
                                                       MAX_ORDER_TOTALPRICE,
                                                       MAX_PERCENT_RANK
                                                       ]
                                           ).fetch_arrow_table()

            logger.debug(f"get_golden_rule_facts - successfully converted SQL result set to PyArrow table.")

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
