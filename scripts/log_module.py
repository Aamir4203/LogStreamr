import logging

def setup_logging(logpath):
    # Create a logger
    logger = logging.getLogger("my_logger")
    logger.setLevel(logging.DEBUG)  # Set the lowest log level
    logger.handlers = []
    # Create file handler for error logs
    error_handler = logging.FileHandler(logpath+"/TRT_error.log")
    error_handler.setLevel(logging.ERROR)  # Log only errors and above
    error_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    error_handler.setFormatter(error_formatter)
    logger.addHandler(error_handler)

    # Create console handler for success logs
    #success_handler = logging.StreamHandler()
    success_handler=logging.FileHandler(logpath+"/TRT_sucess.log")
    success_handler.setLevel(logging.INFO)  # Log info and above to the console
    success_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    success_handler.setFormatter(success_formatter)
    logger.addHandler(success_handler)

    return logger
