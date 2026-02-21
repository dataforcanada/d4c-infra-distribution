import libtorrent as lt
import sys
import os
import urllib.parse

### Edit Me ###
DATA_DIR = "~/Documents/Personal/Projects/dataforcanada/d4c-infra-distribution/data"
DATA_DIR = os.path.expanduser(DATA_DIR)
TRACKER_URL = "https://tracker.labs.dataforcanada.org/announce"
CREATOR = "Data for Canada"
DATASET_ID = "ca_statcan_2021A000011124_d4c-datapkg-statistical_dissemination_areas_cartographic_2021_v0.1.0-beta"
TORRENT_COMMENT = "This is geographic boundaries for Statistics Canada's 2021 Dissemination Area boundaries. These are created in Data for Canada's Statistical data packages (d4c-datapkg-statistical). See https://www.dataforcanada.org/docs/d4c-pkgs/d4c-datapkg-statistical/ for more information"

# Source of truth (AKA long-term storage)
DATASET_ID_SOT = f"{DATASET_ID}.parquet"
DATASET_ID_SOT_PATH= f"{DATA_DIR}/{DATASET_ID_SOT}"

# HTTP URL(s) to the dataset
WEB_SEED_URLS = [
    f"https://data.source.coop/dataforcanada/d4c-datapkg-statistical/processed/{DATASET_ID_SOT}",
    f"https://d4c-pkgs.t3.storage.dev/processed/{DATASET_ID_SOT}"
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

    # Generate the magnet link from the torrent info hash
    torrent_info = lt.torrent_info(torrent_dict)
    info_hash = str(torrent_info.info_hash())
    magnet_link = f"magnet:?xt=urn:btih:{info_hash}"
    magnet_link += f"&dn={urllib.parse.quote(filename)}"
    magnet_link += f"&tr={urllib.parse.quote(TRACKER_URL)}"
    # Add web seeds to the magnet link
    for seed_url in WEB_SEED_URLS:
        magnet_link += f"&ws={urllib.parse.quote(seed_url)}"
    print(f"Success: Generated magnet link for '{filename}'")
    return magnet_link


if __name__ == '__main__':
    if os.path.exists(DATASET_ID_SOT_PATH):
        link = generate_data_package_torrent(DATASET_ID_SOT_PATH, TORRENT_COMMENT, WEB_SEED_URLS)
        print(f"Magnet link: {link}")
    else:
        print(f"Error: File not found: {DATASET_ID_SOT_PATH}")