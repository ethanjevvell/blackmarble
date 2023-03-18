import requests
import json
import sys
import time

start_url = "https://ladsweb.modaps.eosdis.nasa.gov/api/v2/content/details?products=VNP46A2&archiveSets=5000&temporalRanges=2013-01-01..2023-01-01&regions=[TILE]H09V05"

download_links = []
response = requests.get(start_url)

max_retries = 20

while response.status_code == 200:

    retry_count = 0
    json_data = response.json()

    try:
        download_link = json_data["content"][0]["downloadsLink"]
        download_links.append(download_link)

    except:
        if not json_data["nextPageLink"]:
            sys.exit("No next download link")
        else:
            print("there was no content for this day, but the show goes on")

    next_page_url = json_data["nextPageLink"]
    
    while retry_count < max_retries:
        try:
            response = requests.get(next_page_url)
            if response.status_code == 200:
                break
            else:
                raise Exception(f"Server responded with status code {response.status_code}")
        except Exception as e:
            print(f"Error encountered: {e}")
            retry_count += 1
            time.sleep(2**retry_count)  # Exponential backoff

    if retry_count >= max_retries:
        print("Reached maximum retries, exiting.")
        break

    print(download_link)
    print(next_page_url)

with open("download_links.txt", "w") as file:
    for link in download_links:
        file.write(f"{link}\n")
