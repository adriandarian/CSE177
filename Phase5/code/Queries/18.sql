SELECT l_discount 
FROM lineitem, orders, customer, nation, region
WHERE l_orderkey = o_orderkey AND o_custkey = c_custkey AND 
	c_nationkey = n_nationkey AND n_regionkey = r_regionkey AND 
	r_name = 'ASIA' AND c_mktsegment = 'BUILDING' AND c_acctbal < 200.0 AND l_returnflag = 'N' AND l_linestatus = 'F'

