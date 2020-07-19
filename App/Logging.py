import logging

from .Config import Config


class Logging(object):
    debuglog = None
    infolog = None

    def __init__(self, pyContext: str):
        if Config.config:
            logging.basicConfig(level=logging.DEBUG if Config.config.debug else logging.INFO)
            Logging.debuglog = logging.getLogger(pyContext).debug
            Logging.infolog = logging.getLogger(pyContext).info
