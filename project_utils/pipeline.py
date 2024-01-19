from reddit_requests import RedditRequests


r = RedditRequests()
res = r.make_request("python")
for post in res.json()["data"]["children"]:
    print(post["data"])
