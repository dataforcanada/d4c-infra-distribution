import libtorrent as lt
import sys
import os
import urllib.parse

### Edit Me ###
DATA_DIR = "~/Documents/Personal/Projects/dataforcanada/decentralized-distribution-labs/data"
DATA_DIR = os.path.expanduser(DATA_DIR)
TRACKER_URL = "https://tracker.labs.dataforcanada.org/announce"
CREATOR = "Data for Canada"
DATASET_ID = "ca-bc_vancouver-2022A00055915022_orthoimagery_2022_075mm"
TORRENT_COMMENT = "Data for Canada orthoimagery labs data package"
FILEPATH = f"{DATA_DIR}/{DATASET_ID}.pmtiles"

# HTTP URL(s) to the dataset
WEB_SEED_URLS = [
    f"https://data.source.coop/dataforcanada/ca-orthoimagery-labs/{DATASET_ID}.pmtiles",
    f"https://data-01.labs.dataforcanada.org/processed/{DATASET_ID}.pmtiles",
    f"https://data-02.labs.dataforcanada.org/processed/{DATASET_ID}.pmtiles"
]
### Edit Me ###

def generate_data_package_torrent(filepath, TORRENT_COMMENT, WEB_SEED_URLS):
    parent_dir = os.path.dirname(filepath)
    filename = os.path.basename(filepath)
    
    # Create file storage. 
    # Can either add a file, or folder of file(s)
    fs = lt.file_storage()
    lt.add_files(fs, filepath)

    # Create torrent object
    t = lt.create_torrent(fs)
    t.add_tracker(TRACKER_URL)
    t.set_creator(CREATOR)
    t.set_comment(TORRENT_COMMENT)
    
    # Add the HTTP web seed (where the data is)
    for seed_url in WEB_SEED_URLS:
        t.add_url_seed(seed_url)

    # Hash the files
    msg = f"Hashing file(s). '{filepath}'"
    print(msg)
    lt.set_piece_hashes(t, parent_dir)

    # Generate the torrent file content
    torrent_dict = t.generate()
    torrent_bytes = lt.bencode(torrent_dict)

    # Save the .torrent file to DATA_DIR
    torrent_filename = f"{DATA_DIR}/{filename}.torrent"
    with open(torrent_filename, "wb") as f:
        f.write(torrent_bytes)
    print(f"Success: Saved '{torrent_filename}'")


if __name__ == '__main__':
    if os.path.exists(FILEPATH):
        link = generate_data_package_torrent(FILEPATH, TORRENT_COMMENT, WEB_SEED_URLS)
    else:
        print(f"Error: File not found: {FILEPATH}")