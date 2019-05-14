SELECT DISTINCT n_name
FROM nation, customer, orders
WHERE o_orderdate>'1994-08-01' AND o_orderdate<'1995-08-01' AND o_custkey=c_custkey AND
	n_nationkey=c_nationkey

