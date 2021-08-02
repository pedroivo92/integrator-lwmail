import pymysql
from settings import *


class GlobomailRepository:

    def __init__(self):
        self.hostname = GLOBOMAIL_DB_HOST
        self.username = GLOBOMAIL_DB_USER
        self.password = GLOBOMAIL_DB_PASSWORD
        self.database = GLOBOMAIL_DB_NAME
        self.connection = pymysql.connect(host=self.hostname, user=self.username, password=self.password,
                                          database=self.database, charset='utf8mb4',
                                          cursorclass=pymysql.cursors.DictCursor)

    def call_procedure(self, email):
        try:
            self.cursor = self.connection.cursor()
            self.cursor.execute(f"CALL globomail.prc_mail_migration('{email}', @cd_status, @ds_status)")
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
            self.cursor.execute(f"CALL globomail.prc_mail_migration_homom('{email}', '{new_alias}', '{old_alias[0]}', @cd_status, @ds_status)")
            self.cursor.execute(f"SELECT @cd_status, @ds_status")
            return self.cursor.fetchall()

        except Exception as e:
            raise e
        finally:
            self.cursor.close()

    def call_function(self, email):
        try:
            self.cursor = self.connection.cursor()
            self.cursor.execute(f"SELECT globomail.fn_get_user_quota('{email}')")
            return self.cursor.fetchone()

        except Exception as e:
            raise e
        finally:
            self.cursor.close()
