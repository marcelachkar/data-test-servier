-- Premiere partie du test
SELECT
  t_table.date AS date,
  SUM(t_table.prod_price * t_table.prod_qty) AS ventes
FROM
  TRANSACTION as t_table
WHERE
  t_table.date
   BETWEEN CAST("2020-01-01" AS DATE)
   AND CAST("2020-12-31" AS DATE)
GROUP BY t_table.date
ORDER BY t_table.date;
