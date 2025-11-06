
import logging

def get_logger(level=logging.INFO):
    logger = logging.getLogger("queuectl")
    if not logger.handlers:
        h = logging.StreamHandler()
        fmt = logging.Formatter("[%(asctime)s] %(levelname)s %(processName)s %(message)s", "%Y-%m-%dT%H:%M:%SZ")
        h.setFormatter(fmt)
        logger.addHandler(h)
    logger.setLevel(level)
    return logger