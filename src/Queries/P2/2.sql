SELECT SUM(c_acctbal), name 
FROM customer, orders 
WHERE c_custkey = o_custkey AND o_totalprice < 10000
GROUP BY c_name

