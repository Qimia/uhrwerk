#!/usr/bin/python3
import argparse
import shlex
import subprocess
from datetime import datetime
from pathlib import Path

import chevron

FILE_LOC = Path(__file__).parent.absolute()
SPARK_SUBMIT_TEMPLATE_LOCATION = (
    FILE_LOC / "templates" / "spark_submit_template.mustache"
)
SPARK_SUBMIT_TEMPLATE_DOCKER_LOCATION = (
    FILE_LOC / "templates" / "spark_submit_template_docker.mustache"
)
UHRWERK_LOC = FILE_LOC.parent


def get_confdir_locations(confdir_setting, dagmode, table_id):
    """Retrieve configuration options according to config directory
    convention"""

    def path_generator(path_loc):
        return (x for x in path_loc.iterdir() if x.is_dir())

    dir_loc = Path(confdir_setting)
    if not dir_loc.is_dir():
        print("configuration-directory can't be found")
        exit(1)
    uhrwerk_loc = dir_loc / "uhrwerk.yml"
    if not uhrwerk_loc.exists():
        print("uhrwerk config not found")
        exit(1)
    uhrwerk_loc = str(uhrwerk_loc)
    connection_configs = [
        str(x)
        for x in dir_loc.iterdir()
        if (not x.is_dir())
        and (x.stem != "uhrwerk")
        and (x.parts[-1].split(".")[-1] in ["yaml", "yml"])
    ]
    if len(connection_configs) < 1:
        print("Some connection config needs to be present in the config-dir")
        exit(1)

    table_configs = []
    if dagmode:
        for area_dir in path_generator(dir_loc):
            for vert_dir in path_generator(area_dir):
                for tab_dir in path_generator(vert_dir):
                    latest_tab = ""
                    # Note: Currently only loads the latest table
                    # TODO: Relies on alphabetical ordering of version numbers
                    for somef in tab_dir.iterdir():
                        if (somef.is_dir() == False) and (
                            somef.parts[-1].split(".")[-1] in ["yaml", "yml"]
                        ):
                            somef_loc = str(somef)
                            if latest_tab < somef_loc:
                                latest_tab = somef_loc
                    if latest_tab != "":
                        table_configs.append(latest_tab)
    else:
        split_char = "."
        table_parts = table_id.split(split_char)
        area = table_parts[0]
        vertical = table_parts[1]
        table_name = table_parts[2]
        version = split_char.join(table_parts[3:])
        table_config_path = (
            dir_loc
            / area
            / vertical
            / table_name
            / (table_name + "_" + version + ".yml")
        )
        if not table_config_path.exists():
            print("table config not found")
            exit(1)
        table_configs.append(str(table_config_path))

    return (uhrwerk_loc, connection_configs, table_configs)


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
    settings["uhrwerk_user_jar"] = (
        parameters.uhrwerk_jar_location + " " + parameters.usercode_jar
    )

    if load_mode == "loadconfdir":
        uhrwerk_config, conn_configs, table_configs = get_confdir_locations(
            parameters.config_directory, parameters.dag_mode, parameters.table_id
        )
        conn_str = " ".join(map("--cons {}".format, conn_configs))
        table_str = " ".join(map("--tables {}".format, table_configs))
        settings["user_configuration_options"] = conn_str + "\n" + table_str
        settings["global_conf_location"] = uhrwerk_config
    elif load_mode == "loadtable":
        conn_str = " ".join(map("--cons {}".format, parameters.conn_configs))
        table_str = " ".join(map("--tables {}".format, parameters.table_configs))
        settings["user_configuration_options"] = conn_str + "\n" + table_str
        settings["global_conf_location"] = parameters.uhrwerk_config
    elif load_mode == "loaddag":
        dag_str = "--dag {}".format(parameters.dag_config)
        settings["user_configuration_options"] = dag_str
        settings["global_conf_location"] = parameters.uhrwerk_config

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

    if parameters.spark_docker:
        spark_template_location = SPARK_SUBMIT_TEMPLATE_DOCKER_LOCATION
    else:
        spark_template_location = SPARK_SUBMIT_TEMPLATE_LOCATION

    with open(spark_template_location) as gmfile:
        template = gmfile.readlines()
        return chevron.render("".join(template), settings)


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

    if parameters.config_directory is not None:
        if table_given:
            print(
                "Using config directory and skipping individual tables / connections parameters"
            )
        if dag_given:
            print("Using config directory and skipping dag parameter")
        return "loadconfdir"
    elif table_given and dag_given:
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
        "usercode_jar", metavar="USER_JAR", help="location of jar with usercode",
    )
    parser.add_argument(
        "--uhrwerk_config",
        action="store",
        metavar="UC",
        type=str,
        help="location of Uhrwerk configuration",
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
        "--config_directory",
        action="store",
        dest="config_directory",
        metavar="CD",
        type=str,
        help="directory location with configs in standard pattern",
    )
    parser.add_argument(
        "--spark_properties",
        action="store",
        dest="spark_properties",
        metavar="SP",
        type=str,
        default=str(FILE_LOC / "example_spark.conf"),
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
        default=str(
            UHRWERK_LOC
            / "uhrwerk-cli"
            / "target"
            / "uhrwerk-cli-0.1.0-SNAPSHOT-jar-with-dependencies.jar"
        ),
        help="location uhrwerk jar",
    )
    parser.add_argument(
        "--spark_docker",
        action="store_true",
        dest="spark_docker",
        default=False,
        help="Run Spark inside docker",
    )

    parameters = parser.parse_args()
    if parameters.uhrwerk_config is None:
        if parameters.config_directory is None:
            print(
                "When loading individual table files or a dag file,"
                + " always specify a location for the uhrwerk configuration"
            )
            exit(1)
    load_mode = check_load_type(parameters)
    rendered_command = render_template(parameters, load_mode)
    if parameters.dryrun:
        print(rendered_command)
    else:
        print("#TODO")
        run_command(rendered_command)
