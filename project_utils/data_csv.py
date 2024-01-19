from .reddit_requests import RedditRequests
import pandas as pd


class RawDataCsv:
    def __init__(self):
        self.r = RedditRequests()
        self.comments_df = pd.DataFrame()
        self.hot_submissions_df = pd.DataFrame()
        self.new_submissions_df = pd.DataFrame()

    def creat_comments_csv(self):
        print(f'Requesting data from Reddit API for comments.')
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

    def create_submission_data(self, res):
        df = pd.DataFrame()
        for post in res.json()['data']['children']:
            df = df._append({
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
        return df

    def create_submission_csv(self, submission_type):
        if submission_type == 'hot':
            print(f'Requesting data from Reddit API for {submission_type} submissions.')
            res = self.r.make_request(f"https://oauth.reddit.com/r/all/{submission_type}", 100)
            self.hot_submissions_df = self.create_submission_data(res)
            return self.hot_submissions_df
        elif submission_type == 'new':
            print(f'Requesting data from Reddit API for {submission_type} submissions.')
            res = self.r.make_request(f"https://oauth.reddit.com/r/all/{submission_type}", 100)
            self.new_submissions_df = self.create_submission_data(res)
            return self.new_submissions_df

