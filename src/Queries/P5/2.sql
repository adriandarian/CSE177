SELECT SUM(c_acctbal), c_name 
FROM customer, orders 
WHERE c_custkey = o_custkey
GROUP BY c_name

