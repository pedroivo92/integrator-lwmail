import random
import requests
from http import HTTPStatus
from datetime import datetime
from unidecode import unidecode

from settings import *

class CapiHandler:

    def __init__(self, logger):
        self.url = str(CAPI_URL)
        self.logger = logger

    def get_customer_information(self, migration_item, customer_data):

        if migration_item['person_type'] == 'PF':
            customer_information = {
                'name': unidecode(migration_item['name'][:70]),
                'login': migration_item['login'],
                'password': migration_item['password'],
                'person_type': 'natural',
                'cpf': migration_item['cpf'],
                'rg': migration_item['rg']
            }
        elif migration_item['person_type'] == 'PJ':
            customer_information = {
                'name': unidecode(migration_item['company_name'][:70]),
                'login': migration_item['login'],
                'password': migration_item['password'],
                'person_type': 'legal',
                'birthdate': '2000-01-01',
                'cnpj': migration_item['cnpj'],
                'main_contact':{
                    'main': True,
                    'name': unidecode(migration_item['name']),
                    'rg': migration_item['rg'],
                    'cpf': migration_item['cpf']
                }
            }

        customer_information.update(customer_data)

        return customer_information

    def generate_login(self, customer_info):
        customer_name = unidecode(customer_info['name'].replace(" ", "").lower())
        name = customer_name.replace(".", "")
        return f"{name[:11]}{random.randint(0,9999999)}"

    def create_custumer(self, token, payload):
        header = {'Service-Ticket': token}
        try:
            create_customer_url = self.url + "/customers"
            response = requests.post(create_customer_url, json=payload, headers=header, timeout=int(CAPI_TIMEOUT))
            response.raise_for_status()

        except requests.exceptions.RequestException as e:
            error = str(e.response.text)
            self.logger.error(msg=error)
            return False, error

        return True, None

    def get_customer_id(self, token, login):
        header = {'Service-Ticket': token}
        try:
            customer_url = self.url + f"/customers?login={login}"
            response = requests.get(customer_url, headers=header, timeout=int(CAPI_TIMEOUT))
            response.raise_for_status()

        except requests.exceptions.RequestException as e:
            error = str(e.response.text)
            self.logger.error(msg=error)
            return False, error

        return response.json()['entries'][0]['id'], None

