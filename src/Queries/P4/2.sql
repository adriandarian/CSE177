SELECT SUM(c_acctbal), c_name 
FROM customer
WHERE c_custkey > -1 AND c_custkey < 90000
GROUP BY c_name