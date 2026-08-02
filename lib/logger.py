class Log4j2(object):
    def __init__(self, spark):
        # Log4j2 package path in JVM
        log4j = spark._jvm.org.apache.logging.log4j
        self.logger = log4j.LogManager.getLogger("sbdl")

    def warn(self, message):
        self.logger.warn(message)

    def info(self, message):
        self.logger.info(message)

    def error(self, message):
        self.logger.error(message)

    def debug(self, message):
        self.logger.debug(message)