from __future__ import annotations

import logging
import re
from enum import Enum
from typing import ClassVar


class BoldColor(str, Enum):
    PURPLE = "\033[95m"
    CYAN = "\033[96m"
    DARKCYAN = "\033[36m"
    BLUE = "\033[94m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"
    END = "\033[0m"


class CustomFormatter(logging.Formatter):
    """
    https://stackoverflow.com/questions/384076/how-can-i-color-python-logging-output
    """

    grey = "\x1b[38;20m"
    cyan = "\x1b[36;20m"
    green = "\x1b[32;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    bold = "\033[1m"
    reset = "\x1b[0m"
    format = "{color}%(asctime)s - %(name)-25s - %(levelname)-8s -{reset} %(message)s {color}- %(filename)s:%(lineno)d{reset}"

    FORMATS: ClassVar[dict[int, str]] = {
        logging.DEBUG: format.format(color=cyan, reset=reset),
        logging.INFO: format.format(color=green, reset=reset),
        logging.WARNING: format.format(color=yellow, reset=reset),
        logging.ERROR: format.format(color=red, reset=reset),
        logging.CRITICAL: format.format(color=bold_red, reset=reset),
    }

    def format(self, record: logging.LogRecord) -> str:
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        record.msg = re.sub(r"(%i)+", self.bold + "%i" + self.reset, record.msg)
        return formatter.format(record)


logger = logging.getLogger("sploty")

if not logger.hasHandlers():
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(CustomFormatter())
    logger.addHandler(handler)
    logger.propagate = False

if __name__ == "__main__":
    logger.debug("%i rows on %s", 185338, 200000)
    logger.info("%i rows on %s", 185338, 200000)
    logger.warning("%i rows on %s", 185338, 200000)
    logger.error("%i rows on %s", 185338, 200000)
    logger.critical("%i rows on %s", 185338, 200000)
