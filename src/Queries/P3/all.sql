SELECT n_nationkey, n_name
FROM nation 
WHERE n_regionkey = 1

SELECT l_orderkey, l_suppkey, l_partkey
FROM lineitem
WHERE l_orderkey < l_suppkey AND l_orderkey < l_partkey AND l_partkey < l_suppkey

SELECT l_orderkey, l_partkey, l_suppkey 
FROM lineitem
WHERE l_returnflag = 'R' AND l_discount < 0.04 AND l_shipmode = 'MAIL'

SELECT c_name, c_address, c_acctbal 
FROM customer 
WHERE c_name = 'Customer#000070919'

SELECT o_orderdate, o_orderstatus, o_orderpriority, o_shippriority
FROM orders
WHERE o_totalprice > 100.0

SELECT l_orderkey 
FROM lineitem
WHERE l_quantity > 30.0

SELECT s_name, s_phone
FROM supplier
WHERE s_nationkey = 10 AND s_acctbal < 100.0

SELECT p_name, p_mfgr, p_brand, p_type 
FROM part
WHERE p_size > 2 AND p_size < 6 AND p_retailprice > 10.0 AND p_retailprice < 15.0

SELECT ps_partkey, ps_suppkey, ps_availqty, ps_supplycost
FROM partsupp
WHERE ps_partkey = 123 AND ps_suppkey > 10 AND ps_suppkey < 100

SELECT l_extendedprice, l_discount
FROM lineitem
WHERE '1994-01-01' < l_shipdate AND '1994-02-01' > l_shipdate