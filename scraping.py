#### YT  ####
# Libraries
from googleapiclient.discovery import build # Zugriff auf die YouTube-Daten
import pandas as pd

api_key = 'AIzaSyAVFMGcx7z47jV1mUsAO741dIF68jgfJoc' # Persönlicher "Schlüssel", um mit YouTube sprechen
youtube = build('youtube', 'v3', developerKey=api_key) # YouTube-API-Objekt erstellen

# Function to search for YouTube videos using a game title
def search_youtube_videos(query, max_results=1):
    request = youtube.search().list(
        q=query,
        part="snippet",
        type="video",
        maxResults=max_results
    )
    response = request.execute()

    video_ids = []
    for item in response['items']:
        video_id = item['id']['videoId']
        video_ids.append(video_id)
    return video_ids

# Function to get comments from a YouTube video
def get_comments(video_id):
    comments = []


    try:
        request = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=100,
            textFormat="plainText"
        )
        page = 1

        while request:
            print(f"Fetching page {page} for video {video_id}... (so far: {len(comments)} comments)")
            try:
                response = request.execute()
            except Exception as e:
                print(f"Error on page {page}: {e}")
                break  # don't retry forever

            for item in response['items']:
                comment = item['snippet']['topLevelComment']['snippet']['textDisplay']
                comments.append(comment)

            request = youtube.commentThreads().list_next(request, response)
            page += 1

    except Exception as e:
        print(f"Failed to fetch comments for {video_id}: {e}")

    print(f"Finished video {video_id} with {len(comments)} comments.")
    return comments

# Load game titles from CSV
df = pd.read_csv("kafka_games.csv", quotechar='"')  # Make sure the file exists in your working directory
game_titles = df["title"].dropna().tolist()  # Extract non-empty titles


# Loop through each game title, find a video, and get comments
all_comments = []

for title in game_titles:
    print(f"\nSearching for video related to: {title}")
    video_ids = search_youtube_videos(title, max_results=1)
    
    for vid in video_ids:
        print(f"Found video ID: {vid} — collecting comments...")
        comments = get_comments(vid)
        
        for comment in comments:
            all_comments.append({
                "game_title": title,
                "video_id": vid,
                "comment": comment
            })

#Save results to CSV
result_df = pd.DataFrame(all_comments)
result_df.to_csv("kafka_game_youtube_comments.csv", index=False)
print("\nAll comments saved to 'kafka_game_youtube_comments.csv'")
