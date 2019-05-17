SELECT p_name, p_mfgr, p_brand, p_type 
FROM part
WHERE p_size > 2 AND p_size < 6 AND p_retailprice > 10.0 AND p_retailprice < 15.0