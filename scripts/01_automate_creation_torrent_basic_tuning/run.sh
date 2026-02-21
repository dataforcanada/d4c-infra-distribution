DATA_DIR="${HOME}/Documents/Personal/Projects/dataforcanada/d4c-infra-distribution/data"
CORES=$(nproc)

# Piece length (16 MiB) based on https://academictorrents.com/docs/uploading.html
DATASET_ID="ca_versatiles_satellite_2025-08-10"
echo "Creating ${DATASET_ID}.pmtiles.torrent"
py3createtorrent \
    --threads ${CORES} \
    --piece-length 16384 \
    --md5 \
    --source "Data for Canada" \
    --comment "Versatiles satellite data product." \
    --tracker "https://tracker.labs.dataforcanada.org/announce" \
    --webseed "https://data.source.coop/dataforcanada/ca-orthoimagery-labs/${DATASET_ID}.pmtiles" \
    --webseed "https://data-01.labs.dataforcanada.org/processed/${DATASET_ID}.pmtiles" \
    --webseed "https://data-02.labs.dataforcanada.org/processed/${DATASET_ID}.pmtiles" \
    "${DATA_DIR}/${DATASET_ID}.pmtiles" \
    --output "${DATA_DIR}/${DATASET_ID}.pmtiles.torrent" \
    --force

DATASET_ID="ca_nrcan_land_cover_2020_30m"
echo "Creating ${DATASET_ID}.pmtiles.torrent"
py3createtorrent \
    --threads ${CORES} \
    --piece-length 16384 \
    --md5 \
    --source "Data for Canada" \
    --comment "National 30 m resolution land cover classification of Canada (2020) produced by the Canada Centre for Remote Sensing (CCRS) as part of the North American Land Change Monitoring System (NALCMS). / Classification nationale de la couverture terrestre du Canada (2020) à une résolution de 30 m, produite par le Centre canadien de télédétection (CCT) dans le cadre du Système nord-américain de surveillance des changements de la couverture terrestre (NALCMS)." \
    --tracker "https://tracker.labs.dataforcanada.org/announce" \
    --webseed "https://data.source.coop/dataforcanada/ca-orthoimagery-labs/${DATASET_ID}.pmtiles" \
    --webseed "https://data-01.labs.dataforcanada.org/processed/${DATASET_ID}.pmtiles" \
    --webseed "https://data-02.labs.dataforcanada.org/processed/${DATASET_ID}.pmtiles" \
    "${DATA_DIR}/${DATASET_ID}.pmtiles" \
    --output "${DATA_DIR}/${DATASET_ID}.pmtiles.torrent" \
    --force

DATASET_ID="ca-ab_edmonton-2023A00054811061_orthoimagery_2023_075mm"
echo "Creating ${DATASET_ID}.pmtiles.torrent"
py3createtorrent \
    --threads ${CORES} \
    --piece-length 16384 \
    --md5 \
    --source "Data for Canada" \
    --comment "City of Edmonton Orthoimagery for 2023 / Ortho-imagerie de la Ville de Edmonton de 2023" \
    --tracker "https://tracker.labs.dataforcanada.org/announce" \
    --webseed "https://data.source.coop/dataforcanada/ca-orthoimagery-labs/${DATASET_ID}.pmtiles" \
    --webseed "https://data-01.labs.dataforcanada.org/processed/${DATASET_ID}.pmtiles" \
    --webseed "https://data-02.labs.dataforcanada.org/processed/${DATASET_ID}.pmtiles" \
    "${DATA_DIR}/${DATASET_ID}.pmtiles" \
    --output "${DATA_DIR}/${DATASET_ID}.pmtiles.torrent" \
    --force

DATASET_ID="ca-ab_red_deer-2024A00054808011_orthoimagery_2024_075mm"
echo "Creating ${DATASET_ID}.pmtiles.torrent"
py3createtorrent \
    --threads ${CORES} \
    --piece-length 16384 \
    --md5 \
    --source "Data for Canada" \
    --comment "Orthoimagery 7.5cm resolution. / Ortho-imagerie à résolution de 7,5 cm." \
    --tracker "https://tracker.labs.dataforcanada.org/announce" \
    --webseed "https://data.source.coop/dataforcanada/ca-orthoimagery-labs/${DATASET_ID}.pmtiles" \
    --webseed "https://data-01.labs.dataforcanada.org/processed/${DATASET_ID}.pmtiles" \
    --webseed "https://data-02.labs.dataforcanada.org/processed/${DATASET_ID}.pmtiles" \
    "${DATA_DIR}/${DATASET_ID}.pmtiles" \
    --output "${DATA_DIR}/${DATASET_ID}.pmtiles.torrent" \
    --force

DATASET_ID="ca-ab_red_deer-2025A00054808011_orthoimagery_2025_075mm"
echo "Creating ${DATASET_ID}.pmtiles.torrent"
py3createtorrent \
    --threads ${CORES} \
    --piece-length 16384 \
    --md5 \
    --source "Data for Canada" \
    --comment "Orthoimagery 7.5cm resolution. / Ortho-imagerie à résolution de 7,5 cm." \
    --tracker "https://tracker.labs.dataforcanada.org/announce" \
    --webseed "https://data.source.coop/dataforcanada/ca-orthoimagery-labs/${DATASET_ID}.pmtiles" \
    --webseed "https://data-01.labs.dataforcanada.org/processed/${DATASET_ID}.pmtiles" \
    --webseed "https://data-02.labs.dataforcanada.org/processed/${DATASET_ID}.pmtiles" \
    "${DATA_DIR}/${DATASET_ID}.pmtiles" \
    --output "${DATA_DIR}/${DATASET_ID}.pmtiles.torrent" \
    --force

DATASET_ID="ca-bc_burnaby-2020A00055915025_orthoimagery_2020_075mm"
echo "Creating ${DATASET_ID}.pmtiles.torrent"
py3createtorrent \
    --threads ${CORES} \
    --piece-length 16384 \
    --md5 \
    --source "Data for Canada" \
    --comment "Orthoimagery 7.5cm resolution. / Ortho-imagerie à résolution de 7,5 cm." \
    --tracker "https://tracker.labs.dataforcanada.org/announce" \
    --webseed "https://data.source.coop/dataforcanada/ca-orthoimagery-labs/${DATASET_ID}.pmtiles" \
    --webseed "https://data-01.labs.dataforcanada.org/processed/${DATASET_ID}.pmtiles" \
    --webseed "https://data-02.labs.dataforcanada.org/processed/${DATASET_ID}.pmtiles" \
    "${DATA_DIR}/${DATASET_ID}.pmtiles" \
    --output "${DATA_DIR}/${DATASET_ID}.pmtiles.torrent" \
    --force

DATASET_ID="ca-bc_vancouver-2022A00055915022_orthoimagery_2022_075mm"
echo "Creating ${DATASET_ID}.pmtiles.torrent"
py3createtorrent \
    --threads ${CORES} \
    --piece-length 16384 \
    --md5 \
    --source "Data for Canada" \
    --comment "Orthoimagery 7.5cm resolution. / Ortho-imagerie à résolution de 7,5 cm." \
    --tracker "https://tracker.labs.dataforcanada.org/announce" \
    --webseed "https://data.source.coop/dataforcanada/ca-orthoimagery-labs/${DATASET_ID}.pmtiles" \
    --webseed "https://data-01.labs.dataforcanada.org/processed/${DATASET_ID}.pmtiles" \
    --webseed "https://data-02.labs.dataforcanada.org/processed/${DATASET_ID}.pmtiles" \
    "${DATA_DIR}/${DATASET_ID}.pmtiles" \
    --output "${DATA_DIR}/${DATASET_ID}.pmtiles.torrent" \
    --force

DATASET_ID="ca-mb_winnipeg-2024A00054611040_orthoimagery_2024_075mm"
echo "Creating ${DATASET_ID}.pmtiles.torrent"
py3createtorrent \
    --threads ${CORES} \
    --piece-length 16384 \
    --md5 \
    --source "Data for Canada" \
    --comment "Orthoimagery 7.5cm resolution. / Ortho-imagerie à résolution de 7,5 cm." \
    --tracker "https://tracker.labs.dataforcanada.org/announce" \
    --webseed "https://data.source.coop/dataforcanada/ca-orthoimagery-labs/${DATASET_ID}.pmtiles" \
    --webseed "https://data-01.labs.dataforcanada.org/processed/${DATASET_ID}.pmtiles" \
    --webseed "https://data-02.labs.dataforcanada.org/processed/${DATASET_ID}.pmtiles" \
    "${DATA_DIR}/${DATASET_ID}.pmtiles" \
    --output "${DATA_DIR}/${DATASET_ID}.pmtiles.torrent" \
    --force

DATASET_ID="ca-yt_whitehorse-2019A000556001009_orthoimagery_2019_200mm"
echo "Creating ${DATASET_ID}.pmtiles.torrent"
py3createtorrent \
    --threads ${CORES} \
    --piece-length 16384 \
    --md5 \
    --source "Data for Canada" \
    --comment "Orthoimagery 20cm resolution. / Ortho-imagerie à résolution de 20 cm." \
    --tracker "https://tracker.labs.dataforcanada.org/announce" \
    --webseed "https://data.source.coop/dataforcanada/ca-orthoimagery-labs/${DATASET_ID}.pmtiles" \
    --webseed "https://data-01.labs.dataforcanada.org/processed/${DATASET_ID}.pmtiles" \
    --webseed "https://data-02.labs.dataforcanada.org/processed/${DATASET_ID}.pmtiles" \
    "${DATA_DIR}/${DATASET_ID}.pmtiles" \
    --output "${DATA_DIR}/${DATASET_ID}.pmtiles.torrent" \
    --force