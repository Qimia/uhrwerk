SELECT p.*, sa.selling_date
FROM qimia_oltp.sales sa
JOIN qimia_oltp.sales_items si
ON sa.sales_id = si.sales_id
JOIN qimia_oltp.products p
ON si.product_id = p.product_id
WHERE sa.selling_date >= '<lower_bound>'
AND sa.selling_date < '<upper_bound>'