import os
import requests
import configparser
from pipeline_utils.utils import get_config_param


class RedditAccessToken:
    def __init__(self):
        self.config_dict = self.create_config_dict(self.__class__.__name__)
        self.client_id = get_config_param("REDDIT_CLIENT_ID", config_dict=self.config_dict)
        self.client_secret = get_config_param("REDDIT_CLIENT_SECRET", config_dict=self.config_dict)
        self.grant_type = "password"
        self.username = get_config_param("REDDIT_USERNAME", config_dict=self.config_dict)
        self.password = get_config_param("PASSWORD", config_dict=self.config_dict)
        self.post_url = get_config_param("POST_URL", config_dict=self.config_dict)
        self.auth = None
        self.data = None
        self.headers = None
        self.__TOKEN = None

    @staticmethod
    def create_config_dict(name):
        if 'local_config.ini' in os.listdir():
            print('Creating the config for the current class ...')
            config = configparser.ConfigParser()
            config.read('local_config.ini')
            return config[name]
        else:
            print("Config file not found")
            return None

    def prepare_access_token(self):
        self.auth = requests.auth.HTTPBasicAuth(self.client_id, self.client_secret)
        self.data = {
            'grant_type': self.grant_type,
            'username': self.username,
            'password': self.password,
        }

        self.headers = {
            "User-Agent": "MyAPI/0.0.0.1"
        }

        res = requests.post(self.post_url,
                            auth=self.auth,
                            data=self.data,
                            headers=self.headers,
                            )
        self.__TOKEN = res.json()['access_token']

    def oauth(self):
        self.prepare_access_token()
        headers = {**self.headers, **{"Authorization": f"Bearer {self.__TOKEN}"}}
        return headers

