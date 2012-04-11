rm -rf *.json
ogr2ogr -t_srs EPSG:4326 -f GeoJSON boundaries_001.json shapes/Neighborhoods_2010.shp
ogr2ogr -t_srs EPSG:4326 -f GeoJSON boundaries_002.json shapes/ZillowNeighborhoods-MA.shp
