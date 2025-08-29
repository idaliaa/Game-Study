import pandas as pd

from scraping import get_comments


# Load full list of video IDs you plan to scrape
video_df = pd.read_csv("kafka_video_ids.csv")  # has "title", "video_id"

# Try to load existing comments
try:
    comments_df = pd.read_csv("kafka_game_youtube_comments.csv")  # where comments are saved
    print(f"Loaded {len(comments_df)} existing comments.")
    last_scraped_id = comments_df["video_id"].iloc[-1]
    last_scraped_comment = comments_df["comment"].iloc[-1]
    print(f"Last video: {last_scraped_id}")
    print(f"Last comment: {last_scraped_comment[:100]}...")
except FileNotFoundError:
    comments_df = pd.DataFrame(columns=["game_title", "video_id", "comment"])
    last_scraped_id = None
    last_scraped_comment = None
    print("No existing comment file. Starting fresh.")

# Start collecting comments
all_comments = []
start_collecting = last_scraped_id is None  # Start from top if nothing exists yet

for idx, row in video_df.iterrows():
    title = row["title"]
    video_id = row["video_id"]

    if not start_collecting:
        if video_id == last_scraped_id:
            print(f"Found last video: {video_id}. Starting next.")
            start_collecting = True
        else:
            continue  # skip until last_scraped_id is found

    print(f"Scraping comments for {title} — {video_id}")
    comments = get_comments(video_id)

    found_last = video_id != last_scraped_id  # skip only if same video
    added = 0

    for comment in comments:
        if not found_last:
            if comment == last_scraped_comment:
                found_last = True
            continue  # skip until the last comment is found

        all_comments.append({
            "game_title": title,
            "video_id": video_id,
            "comment": comment
        })
        added += 1


# Save after each video
    if all_comments:
        pd.concat([comments_df, pd.DataFrame(all_comments)]).to_csv("kafka_game_youtube_comments.csv", index=False)
        print(f"{added} new comments saved.")
        comments_df = pd.read_csv("kafka_game_youtube_comments.csv")  # reload updated file
        all_comments = []
    else:
        print("No new comments found.")

print("Done scraping new videos.")
