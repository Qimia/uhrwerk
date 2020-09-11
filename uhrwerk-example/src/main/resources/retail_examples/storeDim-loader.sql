SELECT p.*
FROM qimia_oltp.sales sa
JOIN qimia_oltp.stores st
ON sa.store = st.store_id
WHERE sa.selling_date >= '<lower_bound>'
AND sa.selling_date < '<upper_bound>'

