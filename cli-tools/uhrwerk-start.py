#!/usr/bin/python3
import subprocess
import shlex
import argparse
from datetime import datetime

import pystache

SPARK_SUBMIT_TEMPLATE_LOCATION = "templates/spark_submit_template.mustache"


def convert_bool(bool_val):
    """Convert a bool to a string
    for interfacing with other clis"""
    if bool_val:
        return "y"
    else:
        return "n"


def render_template(parameters, load_mode):
    """ render spark-submit template according to 
    parameters """
    settings = {}

    # Converting parameters to param-replace-dict
    settings["table_ident"] = parameters.table_id
    settings["dag_mode"] = convert_bool(parameters.dag_mode)
    settings["overwrite_mode"] = convert_bool(parameters.overwrite)
    settings["parallel"] = parameters.parallel
    settings["continuous_mode"] = convert_bool(parameters.continuous)
    settings["conf_spark"] = parameters.spark_properties
    settings["global_conf_location"] = parameters.uhrwerk_config
    settings["uhrwerk_user_jar"] = (
        parameters.uhrwerk_jar_location + " " + parameters.usercode_jar
    )

    if load_mode == "loadtable":
        conn_str = "--cons {}".format(" ".join(parameters.conn_configs))
        table_str = "--tables {}".format(" ".join(parameters.table_configs))
        settings["user_configuration_options"] = conn_str + "\n" + table_str
    elif load_mode == "loaddag":
        dag_str = "--dag {}".format(parameters.dag_config)
        settings["user_configuration_options"] = dag_str

    time_str = ""
    if parameters.lower_bound is not None:
        time_str += "--start {}".format(parameters.lower_bound)
    if parameters.upper_bound is not None:
        if len(time_str) > 0:
            time_str += "\n"
        time_str += "--end {}".format(parameters.upper_bound)
    settings["time_options"] = time_str

    if parameters.clusterdeploy:
        settings["deploy_mode"] = "cluster"
    else:
        settings["deploy_mode"] = "client"

    with open(SPARK_SUBMIT_TEMPLATE_LOCATION) as gmfile:
        template = gmfile.readlines()
        return pystache.render("".join(template), settings)


def run_command(command_str):
    """run a spark submit command given a parameter NameSpace"""
    lexed_command = shlex.split(command_str)
    print(lexed_command)
    process = subprocess.Popen(lexed_command)
    process.communicate()


def check_load_type(parameters):
    """Check if either table + connection configs are given
    or a full dag config is given"""
    table_given = False
    dag_given = False
    if (parameters.table_configs is not None) and (parameters.conn_configs is not None):
        table_given = True
    if parameters.dag_config is not None:
        dag_given = True
    if table_given and dag_given:
        print("Both Tables and Dag given, ignoring the tables given")
        return "loaddag"
    elif table_given:
        return "loadtable"
    elif dag_given:
        return "loaddag"
    else:
        print("Loading configurations from DB not supported **yet**")
        raise Exception("Neither dag nor table configuration location given")


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
        prog="uhrwerk-start.py",
        description="Run Uhrwerk by giving spark-submit command",
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
    parser.add_argument(
        "usercode_jar", metavar="USER_JAR", help="location of jar with usercode",
    )
    parser.add_argument(
        "--table_configs",
        action="store",
        dest="table_configs",
        metavar="TC",
        nargs="*",
        help="list of table configurations",
    )
    parser.add_argument(
        "--conn_configs",
        action="store",
        dest="conn_configs",
        metavar="CC",
        nargs="*",
        help="list of connection configurations",
    )
    parser.add_argument(
        "--dag_config",
        action="store",
        dest="dag_config",
        metavar="DC",
        type=str,
        help="complete dag configurations",
    )
    parser.add_argument(
        "--spark_properties",
        action="store",
        dest="spark_properties",
        metavar="SP",
        type=str,
        default="example_spark.conf",
        help="location spark properties file with spark-settings",
    )
    parser.add_argument(
        "-c",
        "--continuous",
        action="store_true",
        dest="continuous",
        default=False,
        help="Start continous Job (ignores bounds)",
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
    parser.add_argument(
        "-p",
        "--parallel",
        action="store",
        dest="parallel",
        metavar="PAR",
        default=1,
        type=int,
        help="Number of threads to use",
    )
    parser.add_argument(
        "-o",
        "--overwrite",
        action="store_true",
        dest="overwrite",
        default=False,
        help="overwrite previous uhrwerk-invocations",
    )
    parser.add_argument(
        "--dag_mode",
        action="store_true",
        dest="dag_mode",
        default=False,
        help="start uhrwerk in dag-mode",
    )
    parser.add_argument(
        "--clusterdeploy",
        action="store_true",
        dest="clusterdeploy",
        default=False,
        help="submit with spark cluster deploy mode (instead of client deploy mode)",
    )
    parser.add_argument(
        "--dryrun",
        action="store_true",
        dest="dryrun",
        default=False,
        help="Only print spark-submit command",
    )
    parser.add_argument(
        "--uhrwerk_jar_location",
        action="store",
        metavar="UJL",
        dest="uhrwerk_jar_location",
        type=str,
        default="../uhrwerk-cli/target/uhrwerk-cli-0.1.0-SNAPSHOT-jar-with-dependencies.jar",
        help="location uhrwerk jar",
    )

    parameters = parser.parse_args()
    load_mode = check_load_type(parameters)
    rendered_command = render_template(parameters, load_mode)
    if parameters.dryrun:
        print(rendered_command)
    else:
        print("#TODO")
        run_command(rendered_command)
