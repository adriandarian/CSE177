SELECT DISTINCT c_name 
FROM lineitem, orders, customer, nation, region
WHERE l_orderkey = o_orderkey AND o_custkey = c_custkey AND 
	c_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'EUROPE' AND o_orderstatus = 'P' AND
	o_orderpriority = '1-URGENT' AND l_shipdate > '1995-10-10' AND l_shipdate < '1995-10-15'

