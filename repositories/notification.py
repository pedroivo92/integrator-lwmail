import requests
from http import HTTPStatus

from settings import *

class NotificationHandler:

    def __init__(self, logger):
        self.url = str(NOTIFICATION_URL)
        self.logger = logger

    def notification_service(self, token, item, emails):
        header = {'Service-Ticket': token,
                  'Content-Type': 'application/json'}
        payload = self._create_notification_payload(item, emails)
        try:
            response = requests.post(self.url, json=payload, headers=header, verify=False, timeout=int(NOTIFICATION_TIMEOUT))
            response.raise_for_status()

        except requests.exceptions.RequestException as e:
            error = str(e.response.text)
            self.logger.error(msg=error)
            return False, error

        return True, None

    def _create_notification_payload(self, item, emails):
        email_list = [item['current_email_address']]
        for email in emails:
            email_list.append(email['email_address'])

        return {
            'name': 'customer/pt-br/create',
            'data': {
                'login': item['login'],
                'channels': ['email'],
                'customer_workbench_uri': 'https://centraldocliente.locaweb.com.br',
                'assets_uri': 'https://assets.locaweb.com.br'
            },
            'recipients': [{
                'customer_id': item['customer_id'],
                'emails': email_list
            }],
            'channels': ['email']
        }