
import logging
from logging import handlers

class log_script:
    """Simple logging class"""
    def __init__(self,  log_file, logger):
        """open logs file and write logs tself.log_file.parent.is_dir() o it"""
        try:
            self.log_file = log_file
            self.parent_dir = self.log_file.parent
            if not self.parent_dir.is_dir() or  not self.parent_dir.exists():
                self.parent_dir.mkdir(mode=0o777)
                if not self.log_file.exists():
                    self.log_file.touch(mode=0o777)


            LOG_FILENAME  = str(self.log_file)
            self.logger = logging.getLogger(logger)
            self.logger.setLevel(logging.DEBUG)
            self.handler = logging.handlers.RotatingFileHandler(LOG_FILENAME, maxBytes=1024000, backupCount=100)
            formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s",datefmt='%Y-%m-%d %H:%M:%S')
            self.handler.setFormatter(formatter)
            self.logger.addHandler(self.handler)


        except Exception as e:
            print("error during Log_me.__init__ :: {}".format(e))


    def info(self, log_data):

        try:
            self.logger.info(log_data)
        except Exception as e:
            print(e)

    def error(self, log_data):
        try:
            self.logger.error(" function_name:" + self.function_name + ": " + log_data)
        except Exception as e:
            print(e)

    def warn(self, log_data):
        try:
            self.logger.warn(log_data)
        except Exception as e:
            print(e)

    def critical(self, log_data):
        try:
            self.logger.critical(" function_name:" + self.function_name + ": " + log_data)
        except Exception as e:
            print(e)

    def shutdown(self):
         try:
             self.handler.close()
             self.logger.removeHandler(self.handler)
           #            logging.shutdown()
         except Exception as  e:
             print(e)
