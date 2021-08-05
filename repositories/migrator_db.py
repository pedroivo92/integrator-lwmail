from datetime import datetime
import pytz
from sqlalchemy import create_engine, exc
from sqlalchemy.sql import text, select, update
from settings import *

class MigrationRepository:

    def __init__(self):
        self.engine = create_engine(DATABASE_CONNECTION_URL, pool_recycle=30, pool_pre_ping=True)
    
    def get_pending_migrations(self):
        self.database_conn = self.engine.connect()
        try:
            container_id = 'c' + CONTAINER_NUMBER
            query = "id_globo, id_status, person_type, current_email_address, alias_email_address, " \
                    "new_email_address, login, password, name, company_name, " \
                    "cnpj, cpf, rg, customer_id, cart_id from integratordb.migration " \
                    f"where id_status = {1} and container_id = '{container_id}' "
            data = self.database_conn.execute(select(text(query)))
            scheduled_migration = data.fetchmany(10)

            query = "a.id_globo, a.id_status, a.person_type, a.current_email_address, " \
                    "a.alias_email_address, a.login, a.password, a.new_email_address, " \
                    "a.name, a.company_name, a.cnpj, a.cpf, a.rg, a.customer_id, a.cart_id, b.id_stage " \
                    "from integratordb.migration as a inner join integratordb.process as b " \
                    f"where a.id_globo = b.id_migration and a.container_id = '{container_id}' " \
                    f"and a.id_status = {2} and b.reprocess = {1}"

            processed_migration = self.database_conn.execute(select(text(query)))

            if processed_migration:
                scheduled_migration.extend(processed_migration.fetchmany(5))

            return scheduled_migration
        except Exception as e:
            raise e 
        finally:
            self.database_conn.close()


    def get_customer_informations(self, item_migration):
        self.database_conn = self.engine.connect()
        try:
            customer_info = {}
            query = "city, state, postal_code, country, number, street from integratordb.address " \
                    f"where id_migration = '{item_migration['id_globo']}'"
            address = self.database_conn.execute(select(text(query))).fetchone()
            customer_info['address'] = {
                'street': address['street'],
                'number': address['number'],
                'complement': None,
                'district': None,
                'city': address['city'],
                'state': address['state'],
                'country': address['country'],
                'postal_code': address['postal_code']
            }

            query = f"phone_number from integratordb.phone where id_migration = '{item_migration['id_globo']}'"
            phones = self.database_conn.execute(select(text(query))).fetchall()
            if phones:
                customer_info['phones'] = []
                for phone in phones:
                    customer_info['phones'].append({'number': phone['phone_number'], 'extension': ' '})

            query = "email_address, main, confirmed from integratordb.email " \
                    f"where id_migration = '{item_migration['id_globo']}'"
            emails = self.database_conn.execute(select(text(query))).fetchall()
            if emails:
                customer_info['emails'] = []
                for email in emails:
                    customer_info['emails'].append(
                        {
                            'address': email['email_address'],
                            'main': True if email['main'] == 1 else False,
                            'confirmed': True if email['confirmed'] == 1 else False
                        }
                    )

            return customer_info
        except Exception as e:
            raise e
        finally:
            self.database_conn.close()

    def update_customer_info(self, customer_info, id_globo):
        self.database_conn = self.engine.connect()
        try:
            current_date = datetime.now(tz=pytz.timezone('America/Sao_Paulo')).strftime('%Y-%m-%dT%H:%M:%S.%f')

            query = "update integratordb.migration " \
                    f"set login = '{customer_info['login']}', customer_id = '{customer_info['customer_id']}', " \
                    f"id_status = {2}, status_date = '{current_date}' " \
                    f"where id_globo = '{id_globo}'"

            self.database_conn.execute(text(query))
        except Exception as e:
            raise e
        finally:
            self.database_conn.close()

    def update_migration_process(self, item, id_stage, error = None):
        self.database_conn = self.engine.connect()
        try:
            query = "insert into integratordb.process (id_migration, id_stage, error_description, reprocess) values " \
                    f"('{item['id_globo']}', {id_stage}, '{error}', 0) on duplicate key update " \
                    f"id_stage = {id_stage}, error_description = '{error}', reprocess = 0"

            self.database_conn.execute(text(query))
        except Exception as e:
            raise e
        finally:
            self.database_conn.close()

    def update_reprocess_status(self, item):
        self.database_conn = self.engine.connect()
        try:
            query = "update integratordb.process " \
                    f"set reprocess = 0 " \
                    f"where id_migration = '{item['id_globo']}'"

            self.database_conn.execute(text(query))
        except Exception as e:
            raise e
        finally:
            self.database_conn.close()

    def update_migration_status(self, item, status, new_email=""):
        self.database_conn = self.engine.connect()
        try:
            current_date = datetime.now(tz=pytz.timezone('America/Sao_Paulo')).strftime('%Y-%m-%dT%H:%M:%S.%f')

            query = "update integratordb.migration " \
                    f"set id_status = {status}, status_date = '{current_date}', " \
                    f"new_email_address = '{new_email}' " \
                    f"where id_globo = '{item['id_globo']}'"

            self.database_conn.execute(text(query))
        except Exception as e:
            raise e
        finally:
            self.database_conn.close()

    def update_plan_informations(self, item, cart_id):
        self.database_conn = self.engine.connect()
        try:
            query = "update integratordb.migration " \
                    f"set cart_id = '{cart_id}' " \
                    f"where id_globo = '{item['id_globo']}'"

            self.database_conn.execute(text(query))
        except Exception as e:
            raise e
        finally:
            self.database_conn.close()
    
    def close_connections(self):
        self.engine.dispose()
