import random
import requests
from http import HTTPStatus
from datetime import datetime

from settings import *

class BluebirdHandler:

    def __init__(self, logger):
        self.url = str(BLUEBIRD_URL)
        self.logger = logger

    def create_payment_method(self, customer_id, token):
        header = {'Service-Ticket': token}
        payload = self._create_payment_payload()
        url = self.url + f"/customers/{customer_id}/billing_info"
        try:
            response = requests.put(url, json=payload, headers=header, timeout=int(PAYMENT_TIMEOUT))
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            error = str(e.response.text)
            self.logger.error(msg=error)
            return False, error

        return True, None

    def create_cart(self, customer_id, plan, token):
        header = {'Service-Ticket': token}
        payload = self._create_cart_payload(customer_id, plan)
        url = self.url + f"/carts"
        try:
            response = requests.post(url, json=payload, headers=header, timeout=int(CART_TIMEOUT))
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            error = str(e.response.text)
            self.logger.error(msg=error)
            return False, error

        return response.json()['id'], None

    def checkout_cart(self, cart_id, token):
        header = {'Service-Ticket': token}
        payload = self._create_checkout_payload(cart_id)
        url = self.url + f"/carts/{cart_id}/checkout"
        try:
            response = requests.post(url, json=payload, headers=header, timeout=int(CHECKOUT_TIMEOUT))
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            error = str(e.response.text)
            #error_detail = error_json["detail"].replace("'", "")
            #error = f'{error_json["title"]} - {error_json["status"]}: {error_detail}'
            self.logger.error(msg=error)
            return False, error

        return True, None

    def get_plan(self, quota):
        if quota in range(0, 20481):
            return {
                "plan_id": 'b7c48522-a5b6-4de9-ae2e-78aaaec9d02f',
                "plan_value": 1.00
            }

        if quota in range(20481, 40961):
            return {
                "plan_id": '2fcff535-553d-4b61-955a-4693181678c5',
                "plan_value": 2.00
            }
        
        if quota in range(40961, 61441):
            return {
                "plan_id": '42546dd2-f736-4604-a19d-28115cc8ebe7',
                "plan_value": 3.50
            }

        if quota in range(61441, 81921):
            return {
                "plan_id": '6360640a-4648-4885-910c-d0ec95480da8',
                "plan_value": 4.50
            }
        
        if quota in range(81921, 102401):
            return {
                "plan_id": '8f2fb04d-96f9-44d1-85fb-d3592da625fe',
                "plan_value": 5.50
            }
        
        return {
            "plan_id": 'e2e9f180-655f-4385-b54e-992ff86a8bfe',
            "plan_value": 10.00
        }

    def _create_payment_payload(self):
        return {
            'payment_method': 'boleto',
            'boleto': {'bank': 'itau'}
        }

    def _create_cart_payload(self, customer_id, plan):
        return {
            'customer_code': customer_id,
            'items': [
                {'plan_id': plan['plan_id'],
                 'periodicity': 1,
                 'quantity': 1,
                 'attributes': [{'key': 'purchase_discount', 'value': plan['plan_value']}]
                 }
            ]
        }

    def _create_checkout_payload(self, cart_id):
        return {
            "anti_fraud_items": [
                {
                    "company": "ClearSale",
                    "fingerprint_id": f"{cart_id}"
                },
                {
                    "company": "Konduto",
                    "fingerprint_id": "1100016190"
                }
            ],
            "accepted_terms": [
                "Contrato de Email Locaweb e Email Exchange",
                "Termo Promocional"
            ]
        }