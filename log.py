from loguru import logger


def f_logger():
    """ """
    logger.add("logs/current.log", rotation="5 minutes", retention=10, level="SUCCESS")
    return logger
