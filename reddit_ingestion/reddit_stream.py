import praw
from dotenv import load_dotenv
from pymongo import MongoClient
import os
from datetime import datetime

# Load environment variables
print("Loading environment variables...")
load_dotenv(encoding='utf-8')

# Reddit API Configuration
REDDIT_CLIENT_ID = os.getenv("client_id")
REDDIT_CLIENT_SECRET = os.getenv("client_secret")
REDDIT_USER_AGENT = os.getenv("user_agent")

# MongoDB Configuration
MONGO_URI = os.getenv("uri")
MONGO_DB = os.getenv("mongodb_username")
MONGODB_PASSWORD = os.getenv("mongodb_password")

# Initialize Reddit client
print("Initializing Reddit client...")
reddit = praw.Reddit(
    client_id=REDDIT_CLIENT_ID,
    client_secret=REDDIT_CLIENT_SECRET,
    user_agent=REDDIT_USER_AGENT
)

# Initialize MongoDB connection
print("Connecting to MongoDB...")
client = MongoClient(MONGO_URI)
db = client['lost_media_db']
collection = db['reddit_posts']
print(f"Connected to MongoDB database: {db.name}")

# List of subreddits to monitor
subreddits = ['tipofmytongue', 'lostmedia']

def fetch_and_store(sub, limit=100):
    """Fetch posts from subreddit and store in MongoDB"""
    print(f"\nStarting fetch from r/{sub}...")
    posts_processed = 0
    
    try:
        for post in reddit.subreddit(sub).hot(limit=limit):
            doc = {
                'id': post.id,
                'title': post.title[:50] + "..." if len(post.title) > 50 else post.title,  # Truncate long titles
                'body': post.selftext[:100] + "..." if len(post.selftext) > 100 else post.selftext,
                'created_utc': post.created_utc,
                'score': post.score,
                'num_comments': post.num_comments,
                'subreddit': sub,
                'processed_at': datetime.utcnow()
            }

            if not collection.find_one({"id": post.id}):
                collection.insert_one(doc)
                posts_processed += 1
                print(f"+ New post: {post.id} ({post.title[:30]}...)")
            else:
                print(f"✓ Post exists: {post.id}")

        print(f"Finished r/{sub}. Processed {posts_processed} new posts")
        return posts_processed

    except Exception as e:
        print(f"! Error processing r/{sub}: {str(e)}")
        return 0

if __name__ == '__main__':
    print("\nStarting Reddit to MongoDB pipeline")
    print("="*50)
    total_processed = 0
    
    for sub in subreddits:
        processed = fetch_and_store(sub)
        total_processed += processed

    print("\nPipeline complete")
    print(f"Total new posts processed: {total_processed}")
    print("="*50)