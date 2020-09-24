#!/usr/bin/python3
import re
import argparse

import yaml
import pymysql.cursors

QUERY_PARTITIONS_LOCATION = "templates/query_partitions.sql"


def load_sql_template():
    """Get sql query template"""

    with open(QUERY_PARTITIONS_LOCATION) as qfile:
        return qfile.read()


def load_connection(uhrwerk_config):
    """Load connection information from uhrwerk's configuration"""

    ip_search = re.compile(r"(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})(\:(\d+))?")
    with open(uhrwerk_config, "r") as config_file:
        config = yaml.load(config_file, Loader=yaml.FullLoader)
        config_metastore = config["metastore"]
        search_res = ip_search.search(config_metastore["jdbc_url"])
        if not search_res.group(1):
            raise Exception("Need a valid ipv4 address for connecting to the Metastore")
        host_loc = search_res.group(1)
        if search_res.group(3):
            host_port = int(search_res.group(3))
        else:
            host_port = 3306
        return (host_loc, host_port, config_metastore["user"], config_metastore["pass"])


def run_partition_check(cl_params, db_info, sql_template):
    """Query the database for which partitions are available
    and print out the result"""

    split_char = "."
    table_parts = cl_params.table_id.split(split_char)
    area = table_parts[0]
    vertical = table_parts[1]
    table_name = table_parts[2]
    version = split_char.join(table_parts[3:])

    connection = pymysql.connect(
        host=db_info[0],
        port=db_info[1],
        user=db_info[2],
        password=db_info[3],
        db="UHRWERK_METASTORE",
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )

    try:
        with connection.cursor() as cursor:
            cursor.execute(sql_template, (area, vertical, table_name, version))
            print("Found {} results".format(cursor.rowcount))
            print("Format\t\tDate\t\t\t\tPartitioned")
            for cur_row in cursor:
                print(
                    "{}\t\t{}\t\t{}".format(
                        cur_row["target_format"],
                        str(cur_row["partition_ts"]),
                        bool(cur_row["partitioned"]),
                    )
                )
    finally:
        connection.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="uhrwerk-partitions-table.py",
        description="Check which partitions have been given for a table",
    )
    parser.add_argument(
        "table_id",
        metavar="TABLE_ID",
        help="Table to run as area.vertical.table.version",
    )
    parser.add_argument(
        "uhrwerk_config",
        metavar="GLOBAL_CONF",
        help="location of Uhrwerk configuration",
    )

    params = parser.parse_args()
    db_info = load_connection(params.uhrwerk_config)
    sql_temp = load_sql_template()
    run_partition_check(params, db_info, sql_temp)
