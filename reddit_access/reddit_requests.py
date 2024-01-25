from reddit_access.reddit_access import RedditAccessToken
import requests


class RedditRequests:
    def __init__(self):
        reddit_access = RedditAccessToken()
        headers = reddit_access.oauth()
        self.headers = headers

    def make_request(self, url, limit):
        res = requests.get(url, headers=self.headers, params={"limit": f"{limit}"})

        return res
