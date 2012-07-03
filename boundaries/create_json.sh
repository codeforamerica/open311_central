rm -rf *.json
# baltimore
ogr2ogr -t_srs EPSG:4326 -f GeoJSON boundaries_001.json shapes/Neighborhoods_2010.shp
# boston
ogr2ogr -t_srs EPSG:4326 -f GeoJSON boundaries_002.json shapes/ZillowNeighborhoods-MA.shp
# bloomington
ogr2ogr -s_srs EPSG:2245 -t_srs EPSG:4326 -f GeoJSON boundaries_003.json shapes/NeighborhoodAssoications.shp
# Some boundary files use NAME, we always want Name
perl -pi -e 's/NAME/Name/g' *.json
