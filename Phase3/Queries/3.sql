SELECT l_orderkey, l_partkey, l_suppkey 
FROM lineitem
WHERE l_returnflag='R' AND l_discount<0.04 AND l_shipmode='MAIL'

