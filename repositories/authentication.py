import requests
import re

from repositories.cache import CacheHandler
from settings import *

class AuthRepository:

    def __init__(self, auth_user, auth_pass, auth_url, logger):
        self.cache_connection = CacheHandler()
        self.user = auth_user
        self.password = auth_pass
        self.url = auth_url
        self.logger = logger

    def generate_token_tgt(self, service_name):
        token_tgt_name = self._get_token_tgt_name(service_name)
        #token_tgt = self.cache_connection.get_info(token_tgt_name)
        token_tgt = None
        if not token_tgt:
            token_tgt = self._generate_cas_tgt()
            #self.cache_connection.set_info(
            #    token_info=self._get_cache_info(token_tgt_name, token_tgt, 180)
            #)

        return token_tgt

    def generate_token_st(self, token_tgt, service_name):
        token_st_name = self._get_token_name(service_name)
        #token_st = self.cache_connection.get_info(token_st_name)
        token_st = None
        if not token_st:
            token_st = self._generate_cas_st(token_tgt, service_name)
            #self.cache_connection.set_info(
            #    token_info=self._get_cache_info(token_st_name, token_st, 30)
            #)

        return token_st

    def _generate_cas_tgt(self):
        payload = {'username': self.user, 'password': self.password}
        try:
            cas_tgt_url = self.url + "/v1/tickets"
            r = requests.post(cas_tgt_url, payload, verify=False, timeout=int(AUTH_TIMEOUT))
            if not r.status_code == 201:
                raise Exception(r.text)
        except Exception as e:
            raise e

        return self._parse_tgt(r.text)

    def _generate_cas_st(self, token_tgt, service_name):
        payload = {'service': service_name}
        header = {'Content-Type': 'application/x-www-form-urlencoded'}
        cas_tgt_url = self.url + "/v1/tickets"
        url = cas_tgt_url + '/' + token_tgt
        try:
            r = requests.post(url, payload, headers=header, verify=False, timeout=int(AUTH_TIMEOUT))
            if not r.status_code == 200:
                raise (Exception(r.text))
        except Exception as e:
            raise e

        return r.text

    def _get_cache_info(self, name, value, expire):
        return {
            "token_name": name,
            "token_value": value,
            "token_expire": expire
        }

    def _get_token_name(self, service_name):
        if service_name == str(AUTH_SERVICE_CAPI):
            return 'token_ts_capi'

        if service_name == str(AUTH_SERVICE_BLUEBIRD):
            return 'token_ts_bluebird'

        if service_name == str(AUTH_SERVICE_AKAKO):
            return 'token_ts_akako'

    def _get_token_tgt_name(self, service_name):
        if service_name == str(AUTH_SERVICE_AKAKO):
            return 'token_tgt_akako'

        return 'token_tgt'

    def _parse_tgt(self, text):
        m = re.match(r'.*?tickets/(TGT\-.*?)"', text)
        return m.group(1)