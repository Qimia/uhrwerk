SELECT b.*, r.date
FROM business b
JOIN review r
ON r.business_id = b.id
WHERE r.date >= '<lower_bound>'
AND date < '<upper_bound>'