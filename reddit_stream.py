import praw
from dotenv import load_dotenv
import os

load_dotenv(encoding='utf-8')

REDDIT_CLIENT_ID = os.getenv("client_id")
REDDIT_CLIENT_SECRET = os.getenv("client_secret")
REDDIT_USER_AGENT = os.getenv("user_agent")

MONGO_URI = os.getenv("uri")
MONGO_DB = os.getenv("mongodb_username")
MONGODB_PASSWORD = os.getenv("mongodb_password")


reddit = praw.Reddit(
    client_id = REDDIT_CLIENT_ID,
    client_secret = REDDIT_CLIENT_SECRET,
    user_agent = REDDIT_USER_AGENT

)

subreddit = reddit.subreddit('technology')

for comment in subreddit.stream.comments(skip_existing=True):
    print('New comment:====================================================================================')
  
    print(comment.body)
