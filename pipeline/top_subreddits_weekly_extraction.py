"""
top_subreddits_weekly_extraction.py

Weekly extraction of posts from drug-related subreddits using the Reddit API (PRAW).
Runs continuously on a 7-day cycle, extracting posts from all subreddits in the
database and storing results in MongoDB.

Developer Notes:
    Database Schema (per post):
        post_type           : comment or submission
        id                  : unique post ID
        title               : Reddit post title
        selftext            : Reddit post body text
        score               : number of upvotes
        created_at_cleaned  : datetime of posting
        author              : Reddit account name
        flair               : flair tag associated with post
        subreddit           : subreddit in which post was made
        url                 : URL of post
        over_18             : NSFW flag
        spoiler             : spoiler flag
        medications         : list of NPS keywords matched in post

Last updated: 9/19/2023
"""

import re
import os
import json
import time
import praw
import pandas as pd
from collections import defaultdict
from datetime import datetime, timedelta
from http.client import IncompleteRead
from urllib.error import HTTPError
from urllib3.exceptions import ProtocolError
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
from praw.models import MoreComments
from dotenv import load_dotenv

load_dotenv()

# ── Credentials & configuration ───────────────────────────────────────────────
REDDIT_CLIENT_ID     = os.getenv("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
REDDIT_USERNAME      = os.getenv("REDDIT_USERNAME")
REDDIT_PASSWORD      = os.getenv("REDDIT_PASSWORD")
REDDIT_USER_AGENT    = os.getenv("REDDIT_USER_AGENT", "datascrape")

MONGO_HOST_PRIMARY   = os.getenv("MONGO_HOST_PRIMARY", "mongodb://localhost:27021")
MONGO_HOST_MEDS      = os.getenv("MONGO_HOST_MEDS",    "mongodb://localhost:27022")

KEYWORDS_PATH        = os.path.join(os.path.dirname(__file__),
                                    "../data/keywords/keywords_sample.json")
CYCLE_DAYS           = 7


# ── Keyword utilities ──────────────────────────────────────────────────────────
def keywords_extraction_func(filepath: str):
    """Load keyword variants and build reverse lookup from variant → drug name."""
    with open(filepath, "r") as f:
        meds_to_vars = json.load(f)
    keywords    = [v for variants in meds_to_vars.values() for v in variants]
    vars_to_meds = {v: med for med, variants in meds_to_vars.items() for v in variants}
    return keywords, vars_to_meds


def mentioning_med(text: str, keywords: list, vars_to_meds: dict) -> list:
    """Return list of drug names mentioned in text via keyword matching."""
    tokens        = re.findall(r"([a-z0-9]+)", text.lower())
    words_overlap = set(tokens) & set(keywords)
    return list({vars_to_meds[w] for w in words_overlap})


# ── MongoDB utilities ──────────────────────────────────────────────────────────
def get_subreddits_list(client: MongoClient) -> list:
    """Retrieve all tracked subreddits from the subreddits database."""
    collection = client["subreddits_db"]["subreddits"]
    cursor     = collection.find({}, {"subreddit": 1})
    subs       = [doc["subreddit"] for doc in cursor]
    cursor.close()
    return subs


def get_users(client: MongoClient) -> list:
    """Retrieve all tracked user accounts from the user database."""
    collection = client["user_db"]["users"]
    cursor     = collection.find({}, {"author": 1})
    users      = list(cursor)
    cursor.close()
    return users


def check_new_users(reddit: praw.Reddit, client: MongoClient, username: str) -> None:
    """Add a new account to the user database if not already present."""
    users_list = [u["author"] for u in get_users(client)]
    if username in users_list:
        return
    try:
        user_data = reddit.redditor(username)
        user_info = {
            "author":       username,
            "id":           getattr(user_data, "id",           ""),
            "name":         getattr(user_data, "name",         ""),
            "subreddit":    user_data.subreddit.display_name
                            if hasattr(user_data, "subreddit") else "",
            "created_at":   getattr(user_data, "created_utc", ""),
            "is_suspended": getattr(user_data, "is_suspended", ""),
            "last_updated": str(datetime.now().date()),
            "added_on":     str(datetime.now().date()),
        }
        client["user_db"]["users"].insert_one(user_info)
        print(f"Added new account: {username}")
    except Exception as e:
        print(f"Could not retrieve account info for {username}: {e}")


# ── Post processing ────────────────────────────────────────────────────────────
def check_newdata(created_at: datetime, days: int = 3) -> bool:
    """Return True if the post was created within the last `days` days."""
    return created_at >= datetime.utcnow() - timedelta(days=days)


def comments_extraction(post_id: str, submission_keywords: list,
                         reddit: praw.Reddit, client: MongoClient,
                         client_meds: MongoClient,
                         keywords: list, vars_to_meds: dict) -> None:
    """Extract and store all comments for a given submission."""
    submission = reddit.submission(post_id)
    submission.comments.replace_more(limit=None)
    for comment in submission.comments.list():
        author = ""
        if comment.author and comment.author.name:
            author = str(comment.author.name)
        created_at  = datetime.fromtimestamp(comment.created_utc)
        post_month  = created_at.strftime("%m")
        post_year   = created_at.year
        data = {
            "submission_id":        post_id,
            "parent_id":            comment.parent_id,
            "author":               author,
            "body":                 comment.body,
            "created_at":           str(created_at),
            "id":                   comment.id,
            "subreddit":            comment.subreddit.display_name,
            "medications":          mentioning_med(comment.body, keywords, vars_to_meds),
            "submission_medications": submission_keywords,
        }
        client[f"{post_year}_C"][post_month].insert_one(data)


def pre_processing(post: dict, reddit: praw.Reddit,
                   client: MongoClient, client_meds: MongoClient,
                   keywords: list, vars_to_meds: dict,
                   stored: int) -> int:
    """Enrich, classify, and store a single post document."""
    date_str = post.pop("created_at", None)
    if not date_str:
        return stored
    datetime_object = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
    post["created_at_cleaned"] = str(datetime_object)
    post_month = datetime_object.strftime("%m")
    post_year  = datetime_object.year

    text = post.get("selftext", "") or post.get("text", "")
    post["medications"] = mentioning_med(
        text + " " + post.get("title", ""), keywords, vars_to_meds
    )

    if len(post["medications"]) >= 1:
        check_new_users(reddit, client, post["author"])

    comments_extraction(post["id"], post["medications"],
                        reddit, client, client_meds, keywords, vars_to_meds)
    time.sleep(5)

    post["latest_time"]     = datetime(year=1, month=1, day=1)
    post["collection_site"] = "Emory"
    document = {"data": post}

    client[str(post_year)][post_month].insert_one(document)
    if post["medications"]:
        client_meds[str(post_year)][post_month].insert_one(post)

    return stored + 1


# ── Main extraction loop ───────────────────────────────────────────────────────
def data_extraction(all_subreddits: list, reddit: praw.Reddit,
                    client: MongoClient, client_meds: MongoClient,
                    keywords: list, vars_to_meds: dict) -> None:
    """Iterate over all tracked subreddits and extract recent posts."""
    print(f"Number of subreddits to process: {len(all_subreddits)}")
    stored = 0
    for subreddit_name in set(all_subreddits):
        print(f"Processing: {subreddit_name}")
        time.sleep(20)
        try:
            for post in reddit.subreddit(subreddit_name).new(limit=1000):
                author     = str(post.author) if post.author else ""
                created_at = datetime.fromtimestamp(post.created)
                new_post   = {
                    "id":       post.id,
                    "title":    post.title,
                    "subreddit": post.subreddit.display_name,
                    "url":      post.url,
                    "comments": post.num_comments,
                    "selftext": "".join(post.selftext.split("\n")),
                    "created_at": str(created_at),
                    "author":   author,
                    "flair":    post.author_flair_text,
                    "over_18":  post.over_18,
                    "spoiler":  post.spoiler,
                }
                if check_newdata(created_at):
                    stored = pre_processing(new_post, reddit, client,
                                            client_meds, keywords, vars_to_meds,
                                            stored)
        except HTTPError as e:
            print(f"HTTP error on {subreddit_name}: {e}")
        except ServerSelectionTimeoutError:
            print("MongoDB connection lost — exiting extraction loop.")
            break
        except Exception as e:
            print(f"Unexpected error on {subreddit_name}: {e}")
            time.sleep(10)
    print(f"Total posts stored this cycle: {stored}")


if __name__ == "__main__":
    client      = MongoClient(MONGO_HOST_PRIMARY)
    client_meds = MongoClient(MONGO_HOST_MEDS)

    reddit = praw.Reddit(
        client_id=REDDIT_CLIENT_ID,
        client_secret=REDDIT_CLIENT_SECRET,
        password=REDDIT_PASSWORD,
        user_agent=REDDIT_USER_AGENT,
        username=REDDIT_USERNAME,
    )

    keywords, vars_to_meds = keywords_extraction_func(KEYWORDS_PATH)

    while True:
        cycle_start = time.time()
        next_run    = cycle_start + CYCLE_DAYS * 24 * 60 * 60

        all_subreddits = get_subreddits_list(client)
        data_extraction(all_subreddits, reddit, client, client_meds,
                        keywords, vars_to_meds)

        print(f"Total subreddits in database: {len(all_subreddits)}")
        print("Next run at {}".format(
            time.strftime("%a %b %d %H:%M:%S %z %Y", time.localtime(next_run))
        ))

        wait = next_run - time.time()
        if wait > 0:
            time.sleep(wait)
