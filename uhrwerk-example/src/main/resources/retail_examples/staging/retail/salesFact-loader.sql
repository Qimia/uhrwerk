SELECT si.product_id, si.quantity, s.*
FROM qimia_oltp.sales_items si
JOIN qimia_oltp.sales s
ON si.sales_id = s.sales_id
WHERE s.selling_date >= '<lower_bound>'
AND s.selling_date < '<upper_bound>'