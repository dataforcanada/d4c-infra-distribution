import libtorrent as lt
import sys
import os
import urllib.parse

### Edit Me ###
DATA_DIR = "~/Documents/Personal/Projects/dataforcanada/d4c-infra-distribution/data/storage-no-cache/input"
DATA_DIR = os.path.expanduser(DATA_DIR)
TRACKER_URL = "udp://tracker.opentrackr.org:1337/announce"
CREATOR = "Data for Canada"
DATASET_ID = "ca-on_geospatial-ontario-2024A000235_d4c-datapkg-orthoimagery_2024_16cm_v0.0.1-beta"
TORRENT_COMMENT = "See more information at https://github.com/dataforcanada/d4c-datapkg-orthoimagery/issues/3#issuecomment-3867197437"

# For this dataset, this should create a torrent with ~76,000 pieces
PIECE_SIZE = 64 * 1024 * 1024  # 64 MiB

# Source of truth (AKA long-term storage)
DATASET_ID_SOT = f"{DATASET_ID}"
DATASET_ID_SOT_PATH= f"{DATA_DIR}/{DATASET_ID_SOT}"

# HTTP URL(s) to the dataset
WEB_SEED_URLS = [
    f"https://data.source.coop/dataforcanada/d4c-datapkg-orthoimagery/archive/{DATASET_ID}"
]
### Edit Me ###

def generate_data_package_torrent(filepath, TORRENT_COMMENT, WEB_SEED_URLS):
    parent_dir = os.path.dirname(filepath)
    filename = os.path.basename(filepath)
    
    # Create file storage. 
    # Can either add a file, or folder of file(s)
    fs = lt.file_storage()
    lt.add_files(fs, filepath)

    # Create torrent object with explicit piece size
    t = lt.create_torrent(fs, piece_size=PIECE_SIZE)

    t.add_tracker(TRACKER_URL)
    t.set_creator(CREATOR)
    t.set_comment(TORRENT_COMMENT)
    
    # TODO: Add once Source Cooperative upload is complete
    # Add the HTTP web seed (where the data is)
    #for seed_url in WEB_SEED_URLS:
    #    t.add_url_seed(seed_url)

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
    if os.path.exists(DATASET_ID_SOT_PATH):
        link = generate_data_package_torrent(DATASET_ID_SOT_PATH, TORRENT_COMMENT, WEB_SEED_URLS)
    else:
        print(f"Error: File not found: {DATASET_ID_SOT_PATH}")