DATA_DIR="${HOME}/Projects/dataforcanada/decentralized-distribution-labs/data"
DATASET_ID="ca-ab_edmonton-2023A00054811061_orthoimagery_2023_075mm"
CORES=$(nproc)

# Piece length (8 MiB) based on https://academictorrents.com/docs/uploading.html
echo "Creating ${DATASET_ID}.pmtiles.torrent"
py3createtorrent \
    --threads ${CORES} \
    --private \
    --piece-length 8192 \
    --md5 \
    --source "Data for Canada" \
    --comment "City of Edmonton Orthoimagery for 2023 / Ortho-imagerie de la Ville de Edmonton de 2023" \
    --tracker "https://tracker.labs.dataforcanada.org/announce" \
    --webseed "https://data-02.labs.dataforcanada.org/processed/ca-ab_edmonton-2023A00054811061_orthoimagery_2023_075mm.pmtiles" \
    "${DATA_DIR}/${DATASET_ID}.pmtiles" \
    --output "${DATA_DIR}/${DATASET_ID}.pmtiles.torrent" \
    --force