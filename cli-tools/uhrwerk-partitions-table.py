#!/usr/bin/python3
import re
import argparse
from datetime import datetime
import os
import sys

import yaml
import pystache
import pymysql.cursors

ACTUAL_LOCATION = os.path.dirname(sys.argv[0])
QUERY_PARTITIONS_LOCATION = ACTUAL_LOCATION + "/templates/query_partitions.sql"


def get_query(cl_params):
    where_table = ""
    where_time = ""
    if cl_params.table_id is not None:
        split_char = "."
        table_parts = cl_params.table_id.split(split_char)
        area = table_parts[0]
        where_table += f"WHERE tab.area = \'{area}\'\n"
        vertical = table_parts[1]
        where_table += f"AND tab.vertical = \'{vertical}\'\n"
        table_name = table_parts[2]
        where_table += f"AND tab.name = \'{table_name}\'\n"
        version = split_char.join(table_parts[3:])
        where_table += f"AND tab.version = \'{version}\'"
    if cl_params.lower_bound:
        where_time += f"WHERE par.partition_ts >= \'{str(cl_params.lower_bound)}\'\n"
    if cl_params.upper_bound:
        if len(where_time) > 0:
            where_time += f"AND par.partition_ts < \'{str(cl_params.upper_bound)}\'\n"
        else:
            where_time += f"WHERE par.partition_ts < \'{str(cl_params.upper_bound)}\'\n"
    else:
        where_time = where_time[:-1]

    with open(QUERY_PARTITIONS_LOCATION) as qfile:
        query_template = qfile.read()

        return pystache.render(
            query_template, {"where_table": where_table, "where_time": where_time}
        )


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


def run_partition_check(cl_params, db_info):
    """Query the database for which partitions are available
    and print out the result"""

    sql_query = get_query(cl_params)
    print(sql_query)

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
            cursor.execute(sql_query)
            print("Found {} results".format(cursor.rowcount))
            print("Table\t\tFormat\t\tDate\t\t\t\tPartitioned")
            for cur_row in cursor:
                print(
                    "{}\t\t{}\t\t{}\t\t{}\t\t".format(
                        cur_row["table_name"],
                        cur_row["target_format"],
                        str(cur_row["partition_ts"]),
                        bool(cur_row["partitioned"]),
                    )
                )
    finally:
        connection.close()


def valid_date(date_str):
    """Check if date input is a valid date"""
    try:
        datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S")
        return date_str
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(date_str)
        raise argparse.ArgumentTypeError(msg)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="uhrwerk-partitions-table.py",
        description="Check which partitions have been given for a table",
    )
    parser.add_argument(
        "uhrwerk_config",
        metavar="GLOBAL_CONF",
        help="location of Uhrwerk configuration",
    )
    parser.add_argument(
        "--table_id",
        metavar="TI",
        action="store",
        dest="table_id",
        type=str,
        help="Table to run as area.vertical.table.version",
    )
    parser.add_argument(
        "-l",
        "--lower_bound",
        action="store",
        dest="lower_bound",
        metavar="LB",
        type=valid_date,
        help="Start date YYYY-MM-DDTHH:MM:SS",
    )
    parser.add_argument(
        "-u",
        "--upper_bound",
        action="store",
        dest="upper_bound",
        metavar="UB",
        type=valid_date,
        help="End date YYYY-MM-DDTHH:MM:SS",
    )

    params = parser.parse_args()
    db_info = load_connection(params.uhrwerk_config)
    run_partition_check(params, db_info)
