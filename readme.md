Instructions

1) clone
2) create an .env file with a MONGO_URI variable
3) run with: foreman run python load_static_data.py

(run with foreman to easily pick up env variables)


new endpoint:

a) load shape file (instructions and scripts in boundaries dir)
b) load the static data with: foreman run python load_static_data.py city name
c) load the requests data with:
d) create the distinct data with: foreman run python create_distinct_lists.py bloomington

