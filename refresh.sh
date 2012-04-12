# refresh static data
# (destroy previous table for baltimore)
foreman run python load_static_data.py baltimore -d
foreman run python load_static_data.py boston 
# refresh request data
foreman run python load_request_data.py baltimore -d
foreman run python load_request_data.py boston
# stamp request data with boundary
foreman run python geoize_requests.py

foreman run python print_stats.py
