SELECT l_receiptdate
FROM lineitem, orders, customer
WHERE	l_orderkey = o_orderkey AND o_custkey = c_custkey AND	c_name = 'Customer#000000116' AND
	o_orderdate>'1995-03-10' AND o_orderdate<'1995-03-20'

