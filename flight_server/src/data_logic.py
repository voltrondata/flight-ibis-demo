import ibis
from ibis import _
from config import DUCKDB_DB_FILE

# Constant
INNER_JOIN = "inner"
SEMI_JOIN = "semi"
MAX_ORDER_TOTALPRICE = 500_000.00
MAX_PERCENT_RANK = 0.98


def apply_golden_rules(hash_bucket_num: int,
                       total_hash_buckets: int
                       ) -> ibis.Expr:
    # Get a read-only connection so it is thread-safe
    conn: ibis.BaseBackend = ibis.duckdb.connect(database=DUCKDB_DB_FILE, read_only=True)

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
                    )

    # Filter orders to the hash bucket asked for
    # Filter out orders larger than MAX_ORDER_TOTALPRICE
    orders_prelim = (orders
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

    return golden_rule_facts


if __name__ == '__main__':
    x = apply_golden_rules(hash_bucket_num=1,
                           total_hash_buckets=11)
    print(x.head(n=10).execute())
