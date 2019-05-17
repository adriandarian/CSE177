SELECT SUM(l_discount) 
FROM lineitem
WHERE l_orderkey > 100 AND l_orderkey < 400