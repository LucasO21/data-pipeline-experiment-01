from functions import get_video_ids, get_video_transcripts
import time
import datetime

print("Starting data pipeline at ", datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
print("----------------------------------------------")

# Step 1: extract video IDs
t0 = time.time()
get_video_ids()
t1 = time.time()
print("Step 1: Done")
print("---> Video IDs downloaded in", str(t1-t0), "seconds", "\n")

# Step 2: extract transcripts for videos
t0 = time.time()
get_video_transcripts()
t1 = time.time()
print("Step 2: Done")
print("---> Transcripts downloaded in", str(t1-t0), "seconds", "\n")