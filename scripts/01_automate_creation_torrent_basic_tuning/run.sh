CORES=$(nproc)
echo $CORES

py3createtorrent 
    --threads 32 \
    --tracker "udp://tracker.labs.dataforcanada.org:6969/announce" \
    --webseed "https://data-01.labs.dataforcanada.org/processed/ca-ab_edmonton-2023A00054811061_orthoimagery_2023_075mm.pmtiles" \
    ca-ab_edmonton-2023A00054811061_orthoimagery_2023_075mm.pmtiles \
    --output ca-ab_edmonton-2023A00054811061_orthoimagery_2023_075mm.pmtiles.torrent \
    --force