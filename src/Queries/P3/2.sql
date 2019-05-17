SELECT l_orderkey, l_suppkey, l_partkey
FROM lineitem
WHERE l_orderkey < l_suppkey AND l_orderkey < l_partkey AND l_partkey < l_suppkey