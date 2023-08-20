import os
import sys
from psycopg2 import OperationalError
import im_core.drivers
from im_core.classes.Singleton import Singleton


class Database(metaclass=Singleton):
    def __init__(self):
        try:
            # Environment Variables
            self.DB_CONNECTION = os.environ.get('DB_CONNECTION')
            self.DB_HOST = os.environ.get('DB_HOST')
            self.DB_PORT = os.environ.get('DB_PORT')
            self.DB_DATABASE = os.environ.get('DB_DATABASE')
            self.DB_USERNAME = os.environ.get('DB_USERNAME')
            self.DB_PASSWORD = os.environ.get('DB_PASSWORD')

            self.conn = None

            if self.DB_CONNECTION == "pgsql":
                self.conn = im_core.drivers.PostGres(self.DB_HOST, self.DB_PORT, self.DB_DATABASE, self.DB_USERNAME, self.DB_PASSWORD)
            else:
                print("DB_CONNECTION setting is invalid")
                sys.exit()
        except OperationalError as error:
            print('Issue connecting with database... is it online?')
            print(error)
            sys.exit()
