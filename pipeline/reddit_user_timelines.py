"""
reddit_user_timelines.py

Extracts submission histories for all tracked Reddit accounts and stores
results in MongoDB. Also discovers new subreddits via account activity.
Runs continuously on a 10-day cycle.

Pipeline steps:
    1. Iterate through all tracked accounts in the user database.
    2. check_userdb     : Determine if account is new or due for re-extraction
                          (last extract >= 10 days ago).
    3. get_user_submissions : Extract up to 1,000 most recent submissions per account.
    4. create_user_id   : Generate a numerical user ID from the account username.
    5. get_new_subreddit: Add newly discovered drug-related subreddits to the
                          subreddit tracking list if not already present.

Last updated: 9/26/2023
"""

import re
import os
import json
import time
import praw
import pymongo
from collections import defaultdict
from datetime import datetime, timedelta
from string import ascii_lowercase
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

REDDIT_CLIENT_ID     = os.getenv("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
REDDIT_USERNAME      = os.getenv("REDDIT_USERNAME")
REDDIT_PASSWORD      = os.getenv("REDDIT_PASSWORD")
REDDIT_USER_AGENT    = os.getenv("REDDIT_USER_AGENT", "datascrape")

MONGO_HOST_PRIMARY   = os.getenv("MONGO_HOST_PRIMARY", "mongodb://localhost:27021")
MONGO_HOST_MEDS      = os.getenv("MONGO_HOST_MEDS",    "mongodb://localhost:27022")

KEYWORDS_PATH        = os.path.join(os.path.dirname(__file__),
                                    "../data/keywords/keywords_sample.json")
CYCLE_DAYS           = 10


def keywords_extraction_func(filepath: str):
    """Load keyword variants and build reverse lookup from variant → drug name."""
    with open(filepath, "r") as f:
        meds_to_vars = json.load(f)
    keywords     = [v for variants in meds_to_vars.values() for v in variants]
    vars_to_meds = {v: med for med, variants in meds_to_vars.items() for v in variants}
    return keywords, vars_to_meds


def mentioning_med(text: str, keywords: list, vars_to_meds: dict) -> list:
    """Return list of drug names mentioned in text via keyword matching."""
    tokens        = re.findall(r"([a-z0-9]+)", text.lower())
    words_overlap = set(tokens) & set(keywords)
    return list({vars_to_meds[w] for w in words_overlap})


def get_subreddits_list(client: MongoClient) -> list:
    """Retrieve all tracked subreddits from the subreddits database."""
    collection = client["subreddits_db"]["subreddits"]
    cursor     = collection.find({}, {"subreddit": 1})
    subs       = [doc["subreddit"] for doc in cursor]
    cursor.close()
    return subs


def get_users(client: MongoClient) -> list:
    """Retrieve all tracked accounts from the user database."""
    collection = client["user_db"]["users"]
    cursor     = collection.find({}, {"author": 1})
    users      = list(cursor)
    cursor.close()
    return users


def create_user_id(username: str) -> str:
    """
    Generate a numerical ID from a Reddit username by mapping each
    letter to its alphabetical position and concatenating the results.
    The first 3 digits are used as the MongoDB collection name.
    """
    letter_map = {letter: str(idx)
                  for idx, letter in enumerate(ascii_lowercase, start=1)}
    return "".join(letter_map[c] for c in username.lower() if c in letter_map)


def get_new_subreddit(client: MongoClient,
                      medications: list, subreddit: str) -> int:
    """
    Add a subreddit to the tracking database if it contains drug-related
    content and has not been previously recorded.
    Returns 1 if a new subreddit was added, 0 otherwise.
    """
    subreddits = get_subreddits_list(client)
    if len(medications) > 0 and subreddit not in subreddits:
        collection = client["subreddits_db"]["subreddits"]
        collection.insert_one({
            "subreddit":    subreddit,
            "last_updated": str(datetime(year=1, month=1, day=1)),
            "added_on":     str(datetime.now().date()),
        })
        print(f"Added new subreddit: {subreddit}")
        return 1
    return 0


def check_userdb(reddit: praw.Reddit, client: MongoClient,
                 username: str, days: int) -> str | None:
    """
    Check whether an account needs to be (re-)extracted.

    Returns:
        'new_user'                  — account is new; profile added to DB.
        'latest extract for old User' — account exists but extraction is stale.
        None                        — account was recently extracted; skip.
    """
    collection   = client["user_db"]["users"]
    user_from_db = collection.find_one({"author": username}, no_cursor_timeout=True)

    if not user_from_db:
        try:
            user_data = reddit.redditor(username)
            collection.insert_one({
                "author":       username,
                "id":           getattr(user_data, "id",""),
                "name":         getattr(user_data, "name",""),
                "subreddit":    user_data.subreddit.display_name
                                if hasattr(user_data, "subreddit") else "",
                "created_at":   getattr(user_data, "created_utc", ""),
                "is_suspended": getattr(user_data, "is_suspended", ""),
                "last_updated": str(datetime.now().date()),
            })
        except Exception as e:
            print(f"Could not retrieve profile for {username}: {e}")
        return "new_user"

    last_updated = datetime.strptime(user_from_db["last_updated"], "%Y-%m-%d")
    if last_updated < datetime.utcnow() - timedelta(days=days):
        collection.update_many(
            {"author": username},
            {"$set": {"last_updated": str(datetime.now().date())}}
        )
        return "latest extract for old User"

    return None


def get_user_submissions(reddit: praw.Reddit, client: MongoClient,
                         client_meds: MongoClient, username: str,
                         days: int, keywords: list,
                         vars_to_meds: dict) -> tuple[int, int, int]:
    """
    Extract and store all available submissions for a given account.

    Returns:
        posts_retrieved : total posts fetched from the API
        posts_stored    : total posts written to MongoDB
        new_subreddits  : number of new subreddits discovered
    """
    posts_retrieved  = 0
    posts_stored     = 0
    new_subreddits   = 0

    user = reddit.redditor(username)
    try:
        _ = user.id
    except Exception:
        return 0, 0, 0

    try:
        for link in user.submissions.new(limit=None):
            posts_retrieved += 1
            text = (getattr(link, "selftext", "") or "") + " " + \
                   (getattr(link, "title",    "") or "")
            medications = mentioning_med(text, keywords, vars_to_meds)

            created_utc = int(link.created_utc) if hasattr(link, "created_utc") else None
            if not created_utc:
                continue
            datetime_object = datetime.fromtimestamp(created_utc)
            post_month      = datetime_object.strftime("%m")
            post_year       = datetime_object.year

            data = {
                "selftext":           getattr(link, "selftext",""),
                "title":              getattr(link, "title",""),
                "subreddit":          link.subreddit.display_name
                                      if hasattr(link, "subreddit") else "",
                "created_utc":        link.created_utc
                                      if hasattr(link, "created_utc") else "",
                "id":                 getattr(link, "id",""),
                "over_18":            getattr(link, "over_18",""),
                "score":              getattr(link, "score",""),
                "num_comments":       getattr(link, "num_comments",""),
                "url":                getattr(link, "url",""),
                "is_original_content":getattr(link, "is_original_content",""),
                "author":             username,
                "flair":              getattr(link, "link_flair_text",""),
                "created_at_cleaned": str(datetime_object),
                "medications":        medications,
            }

            if datetime_object < datetime.utcnow() - timedelta(days=days):
                if medications:
                    new_subreddits += get_new_subreddit(
                        client, medications, data["subreddit"]
                    )
                try:
                    client[str(post_year)][post_month].insert_one({"data": data})
                    if medications:
                        client_meds[str(post_year)][post_month].insert_one(data)
                except pymongo.errors.DuplicateKeyError:
                    pass
                posts_stored += 1

            time.sleep(0.1)

        client["user_db"]["users"].update_one(
            {"author": username},
            {"$set": {"last_updated": str(datetime.now().date())}}
        )
        return posts_retrieved, posts_stored, new_subreddits

    except Exception as e:
        print(f"Error processing account {username}: {e}")
        return 0, 0, 0


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
        cycle_start      = time.time()
        next_run         = cycle_start + CYCLE_DAYS * 24 * 60 * 60
        users            = get_users(client)
        total_new_subs   = 0
        user_count       = 0

        for user in users:
            username = user.get("author", "")
            if not username:
                continue
            user_count += 1
            latest_extract = check_userdb(reddit, client, username, CYCLE_DAYS)
            if latest_extract:
                time.sleep(15)
                posts_retrieved, posts_stored, added_subs = get_user_submissions(
                    reddit, client, client_meds, username,
                    CYCLE_DAYS, keywords, vars_to_meds
                )
                print(
                    f"  Account {user_count} ({username}) — "
                    f"stored: {posts_stored}, retrieved: {posts_retrieved}, "
                    f"new subreddits: {added_subs}"
                )
                total_new_subs += added_subs

        print(f"New subreddits added this cycle: {total_new_subs}")
        print("Next run at {}".format(
            time.strftime("%a %b %d %H:%M:%S %z %Y", time.localtime(next_run))
        ))

        wait = next_run - time.time()
        if wait > 0:
            time.sleep(wait)
