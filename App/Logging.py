import logging

from .Config import Config


class Logging(object):
    debuglog = None

    def __init__(self, pyContext):
        if Config.config:
            logging.basicConfig(level=logging.DEBUG if Config.config.debug else logging.INFO)
            Logging.debuglog = logging.getLogger(pyContext).debug
