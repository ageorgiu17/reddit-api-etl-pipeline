from reddit_access import RedditAccessToken
import requests


class RedditRequests:
    def __init__(self):
        reddit_access = RedditAccessToken()
        headers = reddit_access.oauth()
        self.headers = headers

    def make_request(self, subreddit_name):
        res = requests.get(f"https://oauth.reddit.com/r/{subreddit_name}/hot",
                           headers=self.headers, params={"limit": "10"})

        return res
