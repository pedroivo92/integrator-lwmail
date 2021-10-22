import traceback
from cryptography.fernet import Fernet

from repositories.migrator_db import MigrationRepository
from repositories.globomail import GlobomailRepository
from repositories.roundcube import RoundcubeRepository
from repositories.authentication import AuthRepository
from repositories.capi import CapiHandler
from repositories.bluebird import BluebirdHandler
from repositories.akako import AkakoHandler
from repositories.notification import NotificationHandler
from settings import *


class IntegratorService:

    def __init__(self, logger):
        self.migration_repository = None
        self.globomail_repository = None
        self.roundcube_repository = None
        try:
            self.logger = logger
            self.logger.info(msg=f'-----SETUP INTEGRATOR_LWMAIL----')
            
            self.migration_repository = MigrationRepository()
            self.logger.info(msg=f'CONNECTED TO MIGRATION REPOSITORY: {self.migration_repository} ')

            self.globomail_repository = GlobomailRepository()
            self.logger.info(msg=f'CONNECTED TO GLOBOMAIL REPOSITORY: {self.globomail_repository} ')

            self.roundcube_repository = RoundcubeRepository()
            self.logger.info(msg=f'CONNECTED TO ROUNDCUBE REPOSITORY: {self.roundcube_repository} ')

            self.authenticator = AuthRepository(AUTH_USER, AUTH_PASSWORD, AUTH_URL, self.logger)
            self.authenticator_akako = AuthRepository(AUTH_USER_AKAKO, AUTH_PASSWORD_AKAKO, AUTH_URL_AKAKO, self.logger)
            self.capi_handler = CapiHandler(self.logger)
            self.bluebird_handler = BluebirdHandler(self.logger)
            self.akako_handler = AkakoHandler(self.logger)
            self.notification_handler = NotificationHandler(self.logger)
            self.encrypt_session = Fernet(APPLICATION_SECRETS.encode('utf8'))
            self.logger.info(msg=f'-----ENDING SETUP INTEGRATOR_LWMAIL----')

        except Exception as e:
            self._close_connections()
            raise e

    def handler_migrations(self):
        self.logger.info(msg='starting integration process for new email migrations')
        try:
            migration_items = self.migration_repository.get_pending_migrations()
            for item in migration_items:
                item = dict(item)
                item['password'] = self._decrypt_password(item)
                self.logger.info(msg=f'starting migration for id_globo: {item["id_globo"]}')
                if item['id_status'] == 2:
                    self.migration_repository.update_reprocess_status(item)
                    if item['id_stage'] in [1, 2]:
                        customer_info = self._handler_capi_process(item)
                        if not customer_info:
                            continue

                        item.update({'login': customer_info['login']})
                        item.update({'customer_id': customer_info['customer_id']})

                        sucess = self._handler_bluebird_process(item, customer_info)
                        if not sucess:
                            continue

                        sucess = self._handler_akako_process(item)
                        if not sucess:
                            continue

                        sucess = self._handler_globomail_procedures(item)
                        if not sucess:
                            continue

                        sucess = self._handler_roundcube_procedures(item)
                        if not sucess:
                            continue
                        
                        sucess = self._handler_notification_process(item)
                        if not sucess:
                            continue
                            
                        continue
                    elif item['id_stage'] in [3, 4, 5]:
                        sucess = self._handler_bluebird_process(item)
                        if not sucess:
                            continue

                        sucess = self._handler_akako_process(item)
                        if not sucess:
                            continue

                        sucess = self._handler_globomail_procedures(item)
                        if not sucess:
                            continue

                        sucess = self._handler_roundcube_procedures(item)
                        if not sucess:
                            continue
                        
                        sucess = self._handler_notification_process(item)
                        if not sucess:
                            continue
                            
                        continue
                    elif item['id_stage'] == 6:
                        sucess = self._handler_akako_process(item)
                        if not sucess:
                            continue

                        sucess = self._handler_globomail_procedures(item)
                        if not sucess:
                            continue

                        sucess = self._handler_roundcube_procedures(item)
                        if not sucess:
                            continue
                        
                        sucess = self._handler_notification_process(item)
                        if not sucess:
                            continue
                        
                        continue
                    elif item['id_stage'] == 7:
                        sucess = self._handler_globomail_procedures(item)
                        if not sucess:
                            continue

                        sucess = self._handler_roundcube_procedures(item)
                        if not sucess:
                            continue
                        
                        sucess = self._handler_notification_process(item)
                        if not sucess:
                            continue
                        
                        continue
                    elif item['id_stage'] == 8:
                        sucess = self._handler_roundcube_procedures(item)
                        if not sucess:
                            continue
                        
                        sucess = self._handler_notification_process(item)
                        if not sucess:
                            continue
                        
                        continue
                    elif item['id_stage'] == 9:
                        sucess = self._handler_notification_process(item)
                        if not sucess:
                            continue
                        
                        continue

                item.update({'id_stage': 3})
                customer_info = self._handler_capi_process(item)
                if not customer_info:
                    continue

                item.update({'login': customer_info['login']})
                item.update({'customer_id': customer_info['customer_id']})

                sucess = self._handler_bluebird_process(item, customer_info)
                if not sucess:
                    continue

                sucess = self._handler_akako_process(item)
                if not sucess:
                    continue

                sucess = self._handler_globomail_procedures(item)
                if not sucess:
                    continue

                sucess = self._handler_roundcube_procedures(item)
                if not sucess:
                    continue
                
                sucess = self._handler_notification_process(item)
                if not sucess:
                    continue

                continue   
        finally:
            self._close_connections()

    def _get_cached_token(self, service_name):
        token_st = None
        if service_name == str(AUTH_SERVICE_AKAKO):
            token_tgt = self.authenticator_akako.generate_token_tgt(service_name)
            if token_tgt:
                token_st = self.authenticator_akako.generate_token_st(token_tgt, service_name)

            return token_st

        token_tgt = self.authenticator.generate_token_tgt(service_name)
        if token_tgt:
            token_st = self.authenticator.generate_token_st(token_tgt, service_name)

        return token_st

    def _handler_capi_process(self, item):
        customer_id = None
        error = None
        customer_information = {}
        customer_data = self.migration_repository.get_customer_informations(item)
        customer_information.update(self.capi_handler.get_customer_information(item, customer_data))
        login = self.capi_handler.generate_login(customer_information)
        customer_information['login'] = login

        token_capi_st = self._get_cached_token(str(AUTH_SERVICE_CAPI))
        
        self.logger.info(msg=f"CAPI - Create Customer for id_globo: {item['id_globo']}")
        customer_created, error = self.capi_handler.create_custumer(token_capi_st, customer_information)
        if error:
            self.migration_repository.update_migration_process(item, 1, error)
            self.migration_repository.update_migration_status(item, 2)
            return False

        self.logger.info(msg=f"CAPI - Get Customer ID for id_globo: {item['id_globo']}")
        customer_id, error = self.capi_handler.get_customer_id(token_capi_st, customer_information['login'])
        if error:
            self.migration_repository.update_migration_process(item, 2, error)
            self.migration_repository.update_migration_status(item, 2)
            return False

        customer_information.update({'customer_id': customer_id})
        self.migration_repository.update_customer_info(customer_information, item['id_globo'])

        return customer_information

    def _handler_bluebird_process(self, item, customer_info=None):        
        # token_bluebird_st = self._get_cached_token(str(AUTH_SERVICE_BLUEBIRD))
        # if customer_info:
        #     item.update({'customer_id': customer_info['customer_id']})

        # if item['id_stage'] == 5:
        #     sucess = self._checkout_cart(item, token_bluebird_st)
        #     if not sucess:
        #         return False
        # if item['id_stage'] == 4:
        #     cart_id = self._create_cart(item, token_bluebird_st)
        #     if not cart_id:
        #         return False

        #     item['cart_id'] = cart_id
        #     sucess = self._checkout_cart(item, token_bluebird_st)
        #     if not sucess:
        #         return False
        # else:
        #     sucess = self._create_payment_method(item, token_bluebird_st)
        #     if not sucess:
        #         return False

        #     cart_id = self._create_cart(item, token_bluebird_st)
        #     if not cart_id:
        #         return False

        #     item['cart_id'] = cart_id
        #     sucess = self._checkout_cart(item, token_bluebird_st)
        #     if not sucess:
        #         return False

        return True

    def _handler_akako_process(self, item):
        item = self._get_new_email(item)

        token_akako_st = self._get_cached_token(str(AUTH_SERVICE_AKAKO))
        self.logger.info(msg=f"AKAKO - Create Akako Customer for id_globo: {item['id_globo']}")
        sucess, error = self.akako_handler.create_akako_custumer(token_akako_st, item)
        if not sucess:
            self.migration_repository.update_migration_process(item, 6, error)
            if item['id_status'] != 2:
                self.migration_repository.update_migration_status(item, 2)
            return False

        return True
    
    def _handler_notification_process(self, item):
        emails = self.migration_repository.get_emails(item)

        if item['id_status'] != 2:
            self.migration_repository.update_migration_status(item, 2)

        self.migration_repository.update_migration_process(item, 9, 'notification: control error')

        item = self._get_new_email(item)
        
        token_st = self._get_cached_token(str(AUTH_SERVICE_NOTIFICATION))
        self.logger.info(msg=f"NOTIFICATION - Notification process for id_globo: {item['id_globo']}")
        sucess, error = self.notification_handler.notification_service(token_st, item, emails)
        if not sucess:
            self.migration_repository.update_migration_process(item, 9, error)
            return False

        self.migration_repository.update_migration_status(item, 3, item['new_email_address'])
        self.migration_repository.delete_process_registry(item)
        self.logger.info(msg=f'id_globo: {item["id_globo"]} successfully migrated')

        return True

    def _handler_globomail_procedures(self, item):
        self.logger.info(msg=f"GLOBOMAIL PROCEDURE - Globomail procedure for id_globo: {item['id_globo']}")
        
        if item['alias_email_address'] and item['alias_email_address'] != "":
            result = self.globomail_repository.call_homon_procedure(item)
        else:
            result = self.globomail_repository.call_procedure(item['current_email_address'])

        if result[0]['@cd_status'] == 2:
            error = f"Error on Globomail procedure: {result[0]['@ds_status']}"
            self.migration_repository.update_migration_process(item, 7, error)
            if item['id_status'] != 2:
                self.migration_repository.update_migration_status(item, 2)
            return False

        return True

    def _handler_roundcube_procedures(self, item):
        self.logger.info(msg=f"ROUNDCUBE PROCEDURE - Roundcube procedure for id_globo: {item['id_globo']}")
        
        if item['alias_email_address'] and item['alias_email_address'] != "":
            result = self.roundcube_repository.call_homon_procedure(item)
        else:
            result = self.roundcube_repository.call_procedure(item['current_email_address'])

        if result[0]['@cd_status'] == 2:
            error = f"Error on Roundcube procedure: {result[0]['@ds_status']}"
            self.migration_repository.update_migration_process(item, 8, error)
            if item['id_status'] != 2:
                self.migration_repository.update_migration_status(item, 2)
            return False

        return True

    def _create_payment_method(self, item, token_bluebird_st):
        payment_method, error = self.bluebird_handler.create_payment_method(item['customer_id'], token_bluebird_st)
        if error:
            self.migration_repository.update_migration_process(item, 3, error)
            if item['id_status'] != 2:
                self.migration_repository.update_migration_status(item, 2)
            return False

        return True

    def _create_cart(self, item, token_bluebird_st):
        quota = self.globomail_repository.call_function(item['current_email_address'])
        
        plan = self.bluebird_handler.get_plan(quota['quota'])

        cart_id, error = self.bluebird_handler.create_cart(item['customer_id'], plan, token_bluebird_st)
        if error:
            self.migration_repository.update_migration_process(item, 4, error)
            if item['id_status'] != 2:
                self.migration_repository.update_migration_status(item, 2)
            return False

        self.migration_repository.update_plan_informations(item, cart_id)
        return cart_id

    def _checkout_cart(self, item, token_bluebird_st):
        payment_method, error = self.bluebird_handler.checkout_cart(item['cart_id'], token_bluebird_st)
        if error:
            self.migration_repository.update_migration_process(item, 5, error)
            if item['id_status'] != 2:
                self.migration_repository.update_migration_status(item, 2)
            return False

        return True

    def _get_new_email(self, item):
        new_domain = item['current_email_address'].split(sep="@")
        item['new_email_address'] = f"{new_domain[0]}@lwmail.com.br"

        if item['alias_email_address']:
            item['new_email_address'] = f"{item['alias_email_address']}@lwmail.com.br"

        return item

    def _decrypt_password(self, item):
        cipher_pass = self.encrypt_session.decrypt(item['password'].encode('utf8'))
        return cipher_pass.decode('utf8')
    
    def _close_connections(self):
        if self.migration_repository:
            self.migration_repository.close_connections()
            self.logger.info(msg=f'CLOSED MIGRATION REPOSITORY CONNECTION')

        if self.globomail_repository:
            self.globomail_repository.close_connections()
            self.logger.info(msg=f'CLOSED GLOBOMAIL REPOSITORY CONNECTION')

        if self.roundcube_repository:
            self.roundcube_repository.close_connections()
            self.logger.info(msg=f'CLOSED ROUNDCUBE REPOSITORY CONNECTION')
