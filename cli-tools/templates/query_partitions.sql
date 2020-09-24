SELECT
	tabtar.target_format,
	par.partition_ts,
	par.partitioned
FROM (SELECT 
		tab.id as table_id,
		tar.id as target_id,
		tar.format as target_format
	FROM TABLE_ tab
	JOIN TARGET tar
	ON tab.id = tar.table_id
	WHERE tab.area = %s
	AND tab.vertical = %s
	AND tab.name = %s
	AND tab.version = %s) tabtar
JOIN PARTITION_ par
ON tabtar.target_id = par.target_id
ORDER BY tabtar.target_format, par.partition_ts