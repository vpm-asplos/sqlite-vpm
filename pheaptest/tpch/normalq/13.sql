SELECT C_COUNT,COUNT(*) AS CUSTDIST
FROM (SELECT C_CUSTKEY, COUNT(O_ORDERKEY) as C_COUNT
 FROM CUSTOMER left outer join ORDERS on C_CUSTKEY = O_CUSTKEY
 AND O_COMMENT not like '%%special%%requests%%'
 GROUP BY C_CUSTKEY) C_ORDERS
/* AS C_ORDERS (C_CUSTKEY, C_COUNT) */
GROUP BY C_COUNT
ORDER BY CUSTDIST DESC, C_COUNT DESC
