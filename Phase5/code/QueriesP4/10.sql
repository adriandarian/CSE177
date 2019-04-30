SELECT SUM(l_extendedprice * l_discount * (1.0-l_tax)), l_suppkey
FROM lineitem
WHERE l_discount < 0.07 AND l_quantity < 12
GROUP BY l_suppkey

