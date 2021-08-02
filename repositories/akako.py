import random
import requests
from http import HTTPStatus
from datetime import datetime

from settings import *

class AkakoHandler:

    def __init__(self, logger):
        self.url = str(AKAKO_URL)
        self.logger = logger

    def create_akako_custumer(self, token, item):
        header = {'Service-Ticket': token,
                  'Content-Type': 'application/json'}
        payload = self._create_akako_payload(item)
        try:
            create_customer_url = self.url + "/v2/customers"
            response = requests.post(create_customer_url, json=payload, headers=header, timeout=int(AKAKO_TIMEOUT))
            response.raise_for_status()

        except requests.exceptions.HTTPError as e:
            error = e.response.text
            self.logger.error(msg=error)
            return False, error

        return True, None

    def _create_akako_payload(self, item):
        name = item['name'].split(sep=" ")
        uid = item['new_email_address'].split(sep="@")
        return {
            'uid': uid[0],
            'mail': item['new_email_address'],
            'first_name': name[0],
            'last_name': name[1],
            'customer_number': item['customer_id'],
            'password': item['password'],
        }



