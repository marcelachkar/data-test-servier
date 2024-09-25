-- Seconde partie du test
WITH ventes_meuble AS (
  SELECT
    t_table.client_id AS client_id,
    SUM(t_table.prod_price * t_table.prod_qty) AS ventes_meuble
  FROM
    TRANSACTION t_table
    INNER JOIN PRODUCT_NOMENCLATURE pn_table ON t_table.prod_id = pn_table.product_id
  WHERE
    t_table.date BETWEEN '2020-01-01' AND '2020-12-31'
    AND pn_table.product_type = 'MEUBLE'
  GROUP BY
    t_table.client_id
), ventes_deco AS (
  SELECT
    t_table.client_id AS client_id,
    SUM(t_table.prod_price * t_table.prod_qty) AS ventes_deco
  FROM
    TRANSACTION t_table
    INNER JOIN PRODUCT_NOMENCLATURE pn_table ON t_table.prod_id = pn_table.product_id
  WHERE
    t_table.date BETWEEN '2020-01-01' AND '2020-12-31'
    AND pn_table.product_type = 'DECO'
  GROUP BY
    t_table.client_id
)
SELECT
    COALESCE(vm_table.client_id, vd_table.client_id) AS client_id,
    COALESCE(vm_table.ventes_meuble, 0) AS ventes_meuble,
    COALESCE(vd_table.ventes_deco, 0) AS ventes_deco
FROM
    ventes_meuble AS vm_table
FULL OUTER JOIN ventes_deco AS vd_table ON vm_table.client_id = vd_table.client_id;
