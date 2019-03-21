SELECT SUM(l_extendedprice * l_discount) 
FROM lineitem
WHERE l_discount < 0.07 AND l_quantity < 24

