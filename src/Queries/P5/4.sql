SELECT n_nationkey, n_regionkey, r_regionkey
FROM nation, region
WHERE n_regionkey = r_regionkey AND r_name = 'AFRICA'