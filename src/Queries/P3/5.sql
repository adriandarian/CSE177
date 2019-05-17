SELECT o_orderdate, o_orderstatus, o_orderpriority, o_shippriority
FROM orders
WHERE o_totalprice > 100.0