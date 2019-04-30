SELECT DISTINCT l_suppkey, l_shipdate, l_commitdate, l_receiptdate
FROM lineitem
WHERE l_shipdate > '1995-10-10' AND l_shipdate < '1995-10-15'

