from reddit_requests import RedditRequests
import pandas as pd


class RawDataCsv:
    def __init__(self):
        self.r = RedditRequests()
        self.comments_df = pd.DataFrame()
        self.hot_submissions_df = pd.DataFrame()
        self.new_submissions_df = pd.DataFrame()

    def creat_comments_csv(self):

        for idx in range(10):
            print(f'Requesting data from Reddit API. Batch {idx} of 10')
            res = self.r.make_request("https://oauth.reddit.com/r/all/comments", 100)
            for post in res.json()['data']['children']:
                self.comments_df = self.comments_df._append({
                    'author': post['data']['author'],
                    'author_flair_text': post['data']['author_flair_text'],
                    'post_text': 'null',
                    'likes': post['data']['likes'],
                    'subreddit_id': post['data']['subreddit_id'],
                    'created_utc': post['data']['created_utc'],
                    'score': post['data']['score'],
                    'post_url': post['data']['link_url'],
                    'subreddit': post['data']['subreddit'],
                    'parent_id': post['data']['parent_id']
                }, ignore_index=True)
        return self.comments_df

    def create_hot_submissions_csv(self):
        for idx in range(10):
            print(f'Requesting data from Reddit API. Batch {idx} of 10')
            res = self.r.make_request("https://oauth.reddit.com/r/all/hot", 100)
            for post in res.json()['data']['children']:
                self.hot_submissions_df = self.hot_submissions_df._append({
                    'author': post['data']['author'],
                    'author_flair_text': post['data']['author_flair_text'],
                    'post_text': post['data']['title'],
                    'likes': post['data']['likes'],
                    'subreddit_id': post['data']['subreddit_id'],
                    'created_utc': post['data']['created_utc'],
                    'score': post['data']['score'],
                    'post_url': post['data']['url'],
                    'subreddit': post['data']['subreddit'],
                    'parent_id': 'n/a_submissions'
                }, ignore_index=True)
        return self.hot_submissions_df

    def create_new_submission_csv(self):
        for idx in range(10):
            print(f'Requesting data from Reddit API. Batch {idx} of 10')
            res = self.r.make_request("https://oauth.reddit.com/r/all/new", 100)
            for post in res.json()['data']['children']:
                self.new_submissions_df = self.new_submissions_df._append({
                    'author': post['data']['author'],
                    'author_flair_text': post['data']['author_flair_text'],
                    'post_text': post['data']['title'],
                    'likes': post['data']['likes'],
                    'subreddit_id': post['data']['subreddit_id'],
                    'created_utc': post['data']['created_utc'],
                    'score': post['data']['score'],
                    'post_url': post['data']['url'],
                    'subreddit': post['data']['subreddit'],
                    'parent_id': 'n/a_submissions'
                }, ignore_index=True)
        return self.new_submissions_df
