SELECT SUM(l_extendedprice * l_discount * (1.0-l_tax))
FROM lineitem
WHERE l_discount < 0.07 AND l_quantity < 12.0

