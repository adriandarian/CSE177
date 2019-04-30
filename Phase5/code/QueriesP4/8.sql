SELECT DISTINCT l_discount 
FROM lineitem
WHERE l_returnflag = 'N' AND l_linestatus = 'F'

