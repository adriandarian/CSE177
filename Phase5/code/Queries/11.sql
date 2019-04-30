SELECT SUM(ps_supplycost), s_suppkey
FROM part, supplier, partsupp 
WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND s_acctbal < 1000.0 AND p_size < 5
GROUP BY s_suppkey

