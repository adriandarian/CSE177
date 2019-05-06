SELECT n_name, n_regionkey
FROM region, nation
WHERE r_regionkey < n_regionkey AND n_regionkey = 3