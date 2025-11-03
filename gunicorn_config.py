import logging
from logging.handlers import TimedRotatingFileHandler

# Gunicorn configuration
bind = 'zds-prod-pgdb02-01.bo3.e-dialog.com:5000'
workers = 4

# Logging configuration
loglevel = 'info'
accesslog = '/u1/techteam/PFM_CUSTOM_SCRIPTS/APT_TOOL_DB/FRONT_END/LOGS/gunicorn_access.log'
errorlog = '/u1/techteam/PFM_CUSTOM_SCRIPTS/APT_TOOL_DB/FRONT_END/LOGS/gunicorn_error.log'

# Configure a rotating file handler for error logs
def configure_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Create a handler that writes log messages to a file
    handler = TimedRotatingFileHandler(
        '/u1/techteam/PFM_CUSTOM_SCRIPTS/APT_TOOL_DB/FRONT_END/LOGS/gunicorn_error.log',
        when='D',  # Rotate daily
        interval=1,  # Every day
        backupCount=2  # Keep last 2 days
    )
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# Call the logging configuration
configure_logging()

