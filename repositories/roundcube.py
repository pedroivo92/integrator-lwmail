import pymysql
from settings import *


class RoundcubeRepository:

    def __init__(self):
        self.hostname = ROUNDCUBE_DB_HOST
        self.username = ROUNDCUBE_DB_USER
        self.password = ROUNDCUBE_DB_PASSWORD
        self.database = ROUNDCUBE_DB_NAME
        self.connection = pymysql.connect(host=self.hostname, user=self.username, password=self.password,
                                          database=self.database, charset='utf8mb4',
                                          cursorclass=pymysql.cursors.DictCursor)

    def call_procedure(self, email):
        try:
            self.cursor = self.connection.cursor()
            self.cursor.execute(f"CALL roundcube.prc_mail_migration('{email}', @cd_status, @ds_status)")
            self.cursor.execute(f"SELECT @cd_status, @ds_status")
            return self.cursor.fetchall()

        except Exception as e:
            raise e
        finally:
            self.cursor.close()

    def call_homon_procedure(self, item):
        try:
            old_alias = item['current_email_address'].split("@")
            email = item['current_email_address']
            new_alias = item['alias_email_address']

            self.cursor = self.connection.cursor()
            self.cursor.execute(f"CALL roundcube.prc_mail_migration_homom('{email}', '{new_alias}', '{old_alias[0]}', @cd_status, @ds_status)")
            self.cursor.execute(f"SELECT @cd_status, @ds_status")
            return self.cursor.fetchall()

        except Exception as e:
            raise e
        finally:
            self.cursor.close()
