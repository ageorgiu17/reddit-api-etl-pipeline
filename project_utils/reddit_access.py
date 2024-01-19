import os
import requests


# TODO - create reddit class for access token


class RedditAccessToken:
    def __init__(self):
        self.client_id = os.environ.get('REDDIT_CLIENT_ID')
        self.client_secret = os.environ.get('REDDIT_CLIENT_SECRET')
        self.grant_type = "password"
        self.username = os.environ.get('REDDIT_USERNAME')
        self.password = os.environ.get('REDDIT_PASSWORD')
        self.auth = None
        self.data = None
        self.headers = None
        self.__TOKEN = None

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

        res = requests.post('https://www.reddit.com/api/v1/access_token',
                            auth=self.auth, data=self.data, headers=self.headers)
        self.__TOKEN = res.json()['access_token']

    def oauth(self):
        self.prepare_access_token()
        headers = {**self.headers, **{"Authorization": f"Bearer {self.__TOKEN}"}}
        return headers


def main():
    reddit_access = RedditAccessToken()
    headers = reddit_access.oauth()

    res = requests.get("https://oauth.reddit.com/r/python/hot",
                       headers=headers, params={"limit": "10"})

    for post in res.json()["data"]["children"]:
        print(post["data"])


if __name__ == "__main__":
    main()

# TODO - move the request to a new file
