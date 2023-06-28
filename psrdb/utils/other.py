import os
import json
import logging


def setup_logging(
        console=True,
        logfile=False,
        filedir=None,
        filename='psrdb.log',
        level=logging.INFO,
    ):
    """
    Setup log handler for the logger object

    Parameters
    ----------
    console : `boolean`
        Output logging to the command line
    logfile : `boolean`
        Output logging to the log file
    filedir : `str`
        Directory to output logger file to
    filename : `str`
        Name of the output logger file

    Returns
    -------
    logger : logger object
        The modified logger object
    """
    # create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(lineno)-4d - %(levelname)-9s :: %(message)s')
    # Create a logger and set the logging level
    logger = logging.getLogger()
    logger.setLevel(level)

    # Create a console handler and set the logging level if console is True
    if console:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        logger.info("Console logger enabled")

    # Create a file handler and set the logging level if logfile is True
    if logfile:
        if not filedir:
            raise ValueError("Filedir must be specified when enabling logfile output.")
        if not os.path.exists(filedir):
            os.makedirs(filedir)
        file_handler = logging.FileHandler(os.path.join(filedir, filename))
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        logger.info(f"File logging enabled into file: {os.path.join(filedir, filename)}")

        #Check if file already exists, if so, add a demarcator to differentiate among runs
        if os.path.exists(os.path.join(filedir, filename)):
            with open(os.path.join(filedir, filename), 'a') as f:
                f.write(20*"#")
                f.write("\n")

    return logger


def get_graphql_id(response, table, logger):
    """
    Parses the graphql response to return the id of the newly created object
    """
    content = json.loads(response.content)
    logger.debug(content.keys())

    if "errors" in content.keys():
        logger.error(f"Error in GraphQL response: {content['errors']}")
        raise ValueError(f"Error in GraphQL response: {content['errors']}")
    else:
        data = content["data"]
        # Only capitlise first character to preserve camel case
        capitalised = table.capitalize()
        mutation = f"create{capitalised}"
        try:
            return int(data[mutation][table]["id"])
        except KeyError:
            raise KeyError(f"No key ['{mutation}']['{table}']['id'] in {data}")


def get_rest_api_id(response, logger):
    content = json.loads(response.content)
    logger.debug(content.keys())

    if content["errors"] is None:
        try:
            return int(content["id"])
        except KeyError:
            return None
    else:
        logger.error(f"Error in GraphQL response: {content['errors']}")
        return None