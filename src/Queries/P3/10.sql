SELECT l_extendedprice, l_discount
FROM lineitem
WHERE '1994-01-01' < l_shipdate AND '1994-02-01' > l_shipdate