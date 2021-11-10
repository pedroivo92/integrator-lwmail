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
        new_email = item['new_email_address']
        email_list = [item['current_email_address']]
        for email in emails:
            email_list.append(email['email_address'])

        return {
            'name': 'customer/pt-br/create',
            'data': {
                'login': item['login'],
                'channels': ['email'],
                'customer_workbench_uri': self._get_notification_mensenger(new_email),
                'assets_uri': 'https://assets.locaweb.com.br'
            },
            'recipients': [{
                'customer_id': item['customer_id'],
                'emails': email_list
            }],
            'channels': ['email']
        }
    
    def _get_notification_mensenger(self, email):
        return "</a> https://centraldocliente.locaweb.com.br <br> <br> Seu Globomail foi migrado " \
               "com sucesso para o LWMAIL.<br> <br> Para acessar sua caixa de e-mails Lwmail: " \
               "<br><br> 1. Entre em https://lwmail.com.br/ <br> 2. " \
               f" Faça Login com {email} <br> 3. Utilize a senha que definiu no " \
               "momento da sua escolha. <br><br> Se precisar redefinir a sua senha, " \
               "na página https://lwmail.com.br/ clique em: Esqueci Minha Senha. " \
               "<br> <br> Veja o vídeo tutorial de como acessar seu LWMAIL " \
               "<a href='https://ajuda.locaweb.com.br/wiki/como-acessar-o-webmail-lwmail/'>Clique aqui</a> <br> <br> " \
               "<br> <br> Se precisar de ajuda para configurar seu software de email ou " \
               "Smartphone " \
               "<a href='https://ajuda.locaweb.com.br/wiki/como-configurar-o-e-mail-no-windows-lwmail/'>Clique aqui</a> <br> <br> " \
               "E agora você tem um login exclusivo para a Central do Cliente Locaweb.<br> <br>"