import ibis
from config import DUCKDB_DB_FILE


def apply_golden_rules(hash_bucket_num: int,
                       total_hash_buckets: int
                       ) -> ibis.Expr:
    # Get a read-only connection so it is thread-safe
    conn = ibis.duckdb.connect(database=DUCKDB_DB_FILE, read_only=True)

    lineitem = conn.table("lineitem")
    orders = conn.table("orders")

    return conn.table("region")


if __name__ == '__main__':
    x = apply_golden_rules()
    print(x)
