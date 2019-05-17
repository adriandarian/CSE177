SELECT ps_partkey, ps_suppkey, ps_availqty, ps_supplycost
FROM partsupp
WHERE ps_partkey = 123 AND ps_suppkey > 10 AND ps_suppkey < 100