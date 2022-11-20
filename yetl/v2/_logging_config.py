import logging
import logging.config
import yaml
import os

from yaml import YAMLError


"""Returns as of a named logger based on a yaml logging configuration file

The configuration file is called logging.yaml. It's a directory
location is taken from an envrionment variables called DATAPREPHOME.
If this does not exist then it is defaulted to ./config/
"""

log_config_file = os.getenv("YETL_HOME", "./config/")
log_config_file = f"{log_config_file}logging.yaml"
log_config_file = os.path.abspath(log_config_file)

# check that it exists
if not os.path.exists(log_config_file):
    msg = f"Config logging file path does not exist {log_config_file}"
    raise Exception(msg)

# load the logging configuration into the logger
with open(log_config_file, "r") as f:

    try:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)

        # if it errors because of invalid yaml format then
        # provide details so the users can easily find and correct
        # if it's a different exception just let it raise
    except YAMLError as e:

        location = ""
        if hasattr(e, "problem_mark"):
            mark = e.problem_mark
            location = f"Error position ({mark.line}, {mark.column})"

        if hasattr(e, "problem"):
            problem = f"{e.problem}."

        raise Exception(
            f"Invalid yaml format in {log_config_file}. {problem} {location}"
        )
