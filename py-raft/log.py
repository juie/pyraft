import logging


class Logger(object):
    def __init__(self, level=logging.DEBUG, log_file=None):
        self.logger = logging.getLogger(__file__)
        self.logger.setLevel(level)
        if log_file is not None:
            self.set_handler(handler=logging.FileHandler(log_file))
        else:
            self.set_handler()

    def set_level(self, level):
        self.logger.setLevel(level)

    def set_handler(self, handler=logging.StreamHandler()):
        formatter = logging.Formatter(
            "%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def info(self, message):
        formatter = "I | {}".format(message)
        self.logger.info(formatter)

    def debug(self, message):
        formatter = "D | {}".format(message)
        self.logger.debug(formatter)

    def warn(self, message):
        formatter = "W | {}".format(message)
        self.logger.warning(formatter)

    def error(self, message):
        formatter = "E | {}".format(message)
        self.logger.error(formatter)


logger = Logger()