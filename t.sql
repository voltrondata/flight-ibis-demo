WITH _ibis_view_0 AS (
  SELECT
    orders_sql.*,
    HASH(orders_sql.o_orderkey) AS hash_result
  FROM orders_sql
), t0 AS (
  SELECT
    t11.o_orderkey AS o_orderkey,
    t11.o_custkey AS o_custkey,
    t11.o_orderstatus AS o_orderstatus,
    t11.o_totalprice AS o_totalprice,
    t11.o_orderdate AS o_orderdate,
    t11.o_orderpriority AS o_orderpriority,
    t11.o_clerk AS o_clerk,
    t11.o_shippriority AS o_shippriority,
    t11.o_comment AS o_comment,
    t11.hash_result AS hash_result
  FROM _ibis_view_0 AS t11
  WHERE
    t11.o_orderdate BETWEEN CAST('1994-01-01' AS DATE) AND CAST('1995-12-31' AS DATE)
), t1 AS (
  SELECT
    t0.o_orderkey AS o_orderkey,
    t0.o_custkey AS o_custkey,
    t0.o_orderstatus AS o_orderstatus,
    t0.o_totalprice AS o_totalprice,
    t0.o_orderdate AS o_orderdate,
    t0.o_orderpriority AS o_orderpriority,
    t0.o_clerk AS o_clerk,
    t0.o_shippriority AS o_shippriority,
    t0.o_comment AS o_comment,
    t0.hash_result AS hash_result,
    t0.hash_result % CAST(11 AS BIGINT) AS hash_bucket
  FROM t0
), t3 AS (
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
    t1.hash_bucket = CAST(11 AS BIGINT) - CAST(1 AS SMALLINT)
    AND t1.o_totalprice <= CAST(500000.0 AS DOUBLE)
), t5 AS (
  SELECT
    t3.o_orderkey AS o_orderkey,
    t3.o_custkey AS o_custkey,
    t3.o_orderstatus AS o_orderstatus,
    t3.o_totalprice AS o_totalprice,
    t3.o_orderdate AS o_orderdate,
    t3.o_orderpriority AS o_orderpriority,
    t3.o_clerk AS o_clerk,
    t3.o_shippriority AS o_shippriority
  FROM t3
), t8 AS (
  SELECT
    t5.o_orderkey AS o_orderkey,
    t5.o_custkey AS o_custkey,
    t5.o_orderstatus AS o_orderstatus,
    t5.o_totalprice AS o_totalprice,
    t5.o_orderdate AS o_orderdate,
    t5.o_orderpriority AS o_orderpriority,
    t5.o_clerk AS o_clerk,
    t5.o_shippriority AS o_shippriority
  FROM t5
  WHERE
    EXISTS(
      SELECT
        CAST(1 AS SMALLINT) AS anon_1
      FROM _ibis_cache_bclb829ync5v57ifn9qaqwdjd AS t11
      WHERE
        t5.o_custkey = t11.o_custkey
    )
), t2 AS (
  SELECT
    t11.r_regionkey AS r_regionkey,
    t11.r_name AS r_name,
    t11.r_comment AS r_comment
  FROM region AS t11
  WHERE
    (
      NOT t11.r_name IN ('EUROPE')
    )
), t4 AS (
  SELECT
    t11.n_nationkey AS n_nationkey,
    t11.n_name AS n_name,
    t11.n_regionkey AS n_regionkey,
    t11.n_comment AS n_comment
  FROM nation AS t11
  WHERE
    EXISTS(
      SELECT
        CAST(1 AS SMALLINT) AS anon_4
      FROM t2
      WHERE
        t11.n_regionkey = t2.r_regionkey
    )
), t7 AS (
  SELECT
    t11.c_custkey AS c_custkey,
    t11.c_name AS c_name,
    t11.c_address AS c_address,
    t11.c_nationkey AS c_nationkey,
    t11.c_phone AS c_phone,
    t11.c_acctbal AS c_acctbal,
    t11.c_mktsegment AS c_mktsegment,
    t11.c_comment AS c_comment
  FROM customer AS t11
  WHERE
    EXISTS(
      SELECT
        CAST(1 AS SMALLINT) AS anon_3
      FROM t4
      WHERE
        t11.c_nationkey = t4.n_nationkey
    )
), t10 AS (
  SELECT
    t8.o_orderkey AS o_orderkey,
    t8.o_custkey AS o_custkey,
    t8.o_orderstatus AS o_orderstatus,
    t8.o_totalprice AS o_totalprice,
    t8.o_orderdate AS o_orderdate,
    t8.o_orderpriority AS o_orderpriority,
    t8.o_clerk AS o_clerk,
    t8.o_shippriority AS o_shippriority
  FROM t8
  WHERE
    EXISTS(
      SELECT
        CAST(1 AS SMALLINT) AS anon_2
      FROM t7
      WHERE
        t8.o_custkey = t7.c_custkey
    )
), t6 AS (
  SELECT
    t11.p_partkey AS p_partkey,
    t11.p_name AS p_name,
    t11.p_mfgr AS p_mfgr,
    t11.p_brand AS p_brand,
    t11.p_type AS p_type,
    t11.p_size AS p_size,
    t11.p_container AS p_container,
    t11.p_retailprice AS p_retailprice,
    t11.p_comment AS p_comment
  FROM part AS t11
  WHERE
    (
      NOT t11.p_mfgr IN ('Manufacturer#3')
    )
), t9 AS (
  SELECT
    t11.l_orderkey AS l_orderkey,
    t11.l_partkey AS l_partkey,
    t11.l_suppkey AS l_suppkey,
    t11.l_linenumber AS l_linenumber,
    t11.l_quantity AS l_quantity,
    t11.l_extendedprice AS l_extendedprice,
    t11.l_discount AS l_discount,
    t11.l_tax AS l_tax,
    t11.l_returnflag AS l_returnflag,
    t11.l_linestatus AS l_linestatus,
    t11.l_shipdate AS l_shipdate,
    t11.l_commitdate AS l_commitdate,
    t11.l_receiptdate AS l_receiptdate,
    t11.l_shipinstruct AS l_shipinstruct,
    t11.l_shipmode AS l_shipmode,
    t11.l_comment AS l_comment
  FROM lineitem AS t11
  WHERE
    EXISTS(
      SELECT
        CAST(1 AS SMALLINT) AS anon_5
      FROM t6
      WHERE
        t11.l_partkey = t6.p_partkey
    )
)
SELECT
  t10.o_orderkey,
  t10.o_custkey,
  t10.o_orderstatus,
  t10.o_totalprice,
  t10.o_orderdate,
  t10.o_orderpriority,
  t10.o_clerk,
  t10.o_shippriority,
  t9.l_orderkey,
  t9.l_partkey,
  t9.l_suppkey,
  t9.l_linenumber,
  t9.l_quantity,
  t9.l_extendedprice,
  t9.l_discount,
  t9.l_tax,
  t9.l_returnflag,
  t9.l_linestatus,
  t9.l_shipdate,
  t9.l_commitdate,
  t9.l_receiptdate,
  t9.l_shipinstruct,
  t9.l_shipmode,
  t9.l_comment
FROM t10
JOIN t9
  ON t10.o_orderkey = t9.l_orderkey