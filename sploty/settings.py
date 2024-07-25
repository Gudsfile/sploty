from __future__ import annotations

import logging
import re
from typing import ClassVar


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
    format = "{color}%(asctime)s - %(name)s - %(levelname)-8s -{reset} %(message)s (%(filename)s:%(lineno)d)"

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
