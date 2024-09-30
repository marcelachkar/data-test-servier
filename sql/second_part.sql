-- Seconde partie du test
SELECT
    t_table.client_id,
    SUM(CASE
            WHEN pn_table.product_type = 'MEUBLE' THEN (t_table.prod_price * t_table.prod_qty)
            ELSE 0
        END) AS ventes_meuble,
    SUM(CASE
            WHEN pn_table.product_type = 'DECO' THEN (t_table.prod_price * t_table.prod_qty)
            ELSE 0
        END) AS ventes_deco
FROM
    TRANSACTION t_table
    INNER JOIN PRODUCT_NOMENCLATURE pn_table ON t_table.prod_id = pn_table.product_id
WHERE
    t_table.date BETWEEN '2020-01-01' AND '2020-12-31'
GROUP BY
    t_table.client_id;
