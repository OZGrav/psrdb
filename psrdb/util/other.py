import os
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