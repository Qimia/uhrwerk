SELECT
	tabtar.table_name,
	tabtar.target_format,
	par.partition_ts,
	par.partitioned
FROM (SELECT 
		tab.id as table_id,
		tab.name as table_name,
		tar.id as target_id,
		tar.format as target_format
	FROM TABLE_ tab
	JOIN TARGET tar
	ON tab.id = tar.table_id
	{{{where_table}}} ) tabtar
JOIN PARTITION_ par
ON tabtar.target_id = par.target_id
{{{where_time}}}
ORDER BY tabtar.table_name, tabtar.target_format, par.partition_ts;