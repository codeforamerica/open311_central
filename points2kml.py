import argparse, json

''' import a json file, in each document, look for a lat, lon field, take it and export as kml file with points '''

def find_point_data(document, lat_field, lon_field):
    return (document[lat_field], document[lon_field])

def load_file(file_path):
    return json.loads(open(file_path, 'r').read())

def convert_json_to_kml(json_data, lat_field, lon_field):
    file_handle = open('output.kml', 'w')
    file_handle.write('<?xml version="1.0" encoding="UTF-8"?>')
    file_handle.write('<kml xmlns="http://www.opengis.net/kml/2.2">')
    file_handle.write('<Document>' )
    points = [find_point_data(doc, lat_field, lon_field) for doc in json_data]
    for point in points:
       file_handle.write('<Placemark>')
       file_handle.write('<Point><coordinates>%s,%s,0</coordinates></Point>' % (point[1], point[0]))
       file_handle.write('</Placemark>')
    file_handle.write('</Document>')
    file_handle.write('</kml>')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Convert arbitrary json file (with lat/lon) to KML")
    parser.add_argument('file_path', action='store', help='json file path')
    parser.add_argument('lat_field', action='store', help='key field for latitude value in json file')
    parser.add_argument('lon_field', action='store', help='key field for longitude value in json file')
    args = parser.parse_args()

    json_data = load_file(args.file_path)
    convert_json_to_kml(json_data, args.lat_field, args.lon_field)

