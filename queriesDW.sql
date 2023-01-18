-- Select Warehouse 
USE dw;

-- Question 1
SELECT store.STORE_NAME AS "STORE NAME", SUM(trans.SALE) AS "TOTAL SALES"
FROM STORES store, TRANSACTIONS trans, TIMES t
WHERE trans.STORE_ID = store.STORE_ID
AND trans.TIME_ID = t.TIME_ID
AND t.MONTH = 9
AND t.YEAR = 2017
AND trans.STORE_ID = store.STORE_ID
GROUP BY store.STORE_ID
ORDER BY SUM(trans.SALE) DESC
LIMIT 3;

-- Question 2
SELECT sup.SUPPLIER_NAME AS "SUPPLIER NAME", SUM(trans.SALE) AS "SALES OVER WEEKENDS"
FROM TIMES t, SUPPLIERS sup, TRANSACTIONS trans
WHERE t.TIME_ID = trans.TIME_ID
AND DAYNAME(STR_TO_DATE(CONCAT(t.YEAR,'-',LPAD(t.MONTH,2,'00'),'-',LPAD(t.DAY,2,'00')), '%Y-%m-%d')) IN("Saturday", "Sunday")
AND sup.SUPPLIER_ID = trans.SUPPLIER_ID
GROUP BY sup.SUPPLIER_NAME
ORDER BY SUM(trans.SALE) DESC
LIMIT 10;
-- Top supplier for the next weekend can be found by the fact which suppliers have performed
-- the best during the previous weekend and overall best performers on weekends.

-- Question 3
SELECT sup.SUPPLIER_NAME AS "SUPPLIER NAME", t.MONTH AS "MONTH", QUARTER(t.TIME_DATE) AS "QUARTER", SUM(trans.SALE) AS "TOTAL SALES" 
FROM SUPPLIERS sup, TIMES t, TRANSACTIONS trans
WHERE sup.SUPPLIER_ID = trans.SUPPLIER_ID
AND trans.TIME_ID = t.TIME_ID
GROUP BY sup.SUPPLIER_ID, t.MONTH, QUARTER(t.TIME_DATE)
ORDER BY sup.SUPPLIER_NAME, t.MONTH, QUARTER(t.TIME_DATE);


-- Question 4
SELECT store.STORE_NAME AS "STORE NAME", prod.PRODUCT_NAME AS "PRODUCT NAME", SUM(trans.SALE) AS "TOTAL SALES"
FROM STORES store, TRANSACTIONS trans, PRODUCTS prod
WHERE store.STORE_ID = trans.STORE_ID
AND trans.PRODUCT_ID = prod.PRODUCT_ID
GROUP BY store.STORE_ID, prod.PRODUCT_ID
ORDER BY store.STORE_NAME, prod.PRODUCT_NAME;

-- Question 5
SELECT store.STORE_NAME AS "STORE NAME", QUARTER(t.TIME_DATE) AS "QUARTER", SUM(trans.SALE) AS "TOTAL SALES"
FROM STORES store, TRANSACTIONS trans, TIMES t
WHERE store.STORE_ID = trans.STORE_ID
AND trans.TIME_ID = t.TIME_ID
GROUP BY store.STORE_ID, QUARTER(t.TIME_DATE)
ORDER BY store.STORE_ID, QUARTER(t.TIME_DATE);

-- Question 6
SELECT prod.PRODUCT_NAME AS "PRODUCT NAME", SUM(trans.SALE) AS "SALES ON WEEKENDS"
FROM PRODUCTS prod, TIMES t, TRANSACTIONS trans
WHERE prod.PRODUCT_ID = trans.PRODUCT_ID
AND t.TIME_ID = trans.TIME_ID
AND DAYNAME(STR_TO_DATE(CONCAT(t.YEAR,'-',LPAD(t.MONTH,2,'00'),'-',LPAD(t.DAY,2,'00')), '%Y-%m-%d')) IN("Saturday", "Sunday")
GROUP BY prod.PRODUCT_NAME
ORDER BY SUM(trans.SALE) DESC
LIMIT 5;

-- Question 7
SELECT store.STORE_NAME AS "STORE", sup.SUPPLIER_NAME AS "SUPPLIER", prod.PRODUCT_NAME AS "PRODUCT", SUM(trans.SALE) AS "SALES"
FROM STORES store, SUPPLIERS sup, PRODUCTS prod, TRANSACTIONS trans
WHERE store.STORE_ID = trans.STORE_ID
AND prod.PRODUCT_ID = trans.PRODUCT_ID
AND sup.SUPPLIER_ID = trans.SUPPLIER_ID
GROUP BY prod.PRODUCT_NAME, store.STORE_NAME, sup.SUPPLIER_NAME WITH ROLLUP;

-- Question 8
SELECT prod.PRODUCT_NAME AS "PRODUCT NAME", CEILING(t.MONTH / 6) AS "HALF", t.YEAR AS "YEAR", SUM(trans.SALE) AS "TOTAL SALES"
FROM PRODUCTS prod, TIMES t, TRANSACTIONS trans
WHERE prod.PRODUCT_ID = trans.PRODUCT_ID
AND trans.TIME_ID = t.TIME_ID
AND t.YEAR = 2017
GROUP BY prod.PRODUCT_NAME, CEILING(t.MONTH / 6), t.YEAR WITH ROLLUP;

-- Question 9
SELECT PRODUCT_NAME, COUNT(*) cnt FROM PRODUCTS GROUP BY PRODUCT_NAME HAVING cnt > 1;
-- One product (Tomatoes) has the same name but different product ID.

-- Question 10
CREATE TABLE IF NOT EXISTS STORE_PRODUCT_ANALYSIS AS
SELECT store.STORE_NAME AS "STORE", prod.PRODUCT_NAME AS "PRODUCT", SUM(trans.SALE) AS "TOTAL SALES"
FROM STORES store, PRODUCTS prod, TRANSACTIONS trans
WHERE store.STORE_ID = trans.STORE_ID
AND prod.PRODUCT_ID = trans.PRODUCT_ID
GROUP BY store.STORE_NAME, prod.PRODUCT_NAME
ORDER BY store.STORE_NAME, prod.PRODUCT_NAME;
-- Show Materialized View
SELECT * FROM STORE_PRODUCT_ANALYSIS;