SELECT e.*, sa.selling_date
FROM qimia_oltp.sales sa
JOIN qimia_oltp.stores_employees se
ON sa.store = se.store_id
JOIN qimia_oltp.employees e
ON se.employee_id = e.employee_id
WHERE sa.selling_date >= '<lower_bound>'
AND sa.selling_date < '<upper_bound>'