CREATE OR REPLACE FUNCTION retrieve_hospitals()
RETURNS variant
LANGUAGE PYTHON
RUNTIME_VERSION = 3.9
--You need to create a access integration to access the api. 
--Check the documentation https://docs.snowflake.com/en/developer-guide/external-network-access
EXTERNAL_ACCESS_INTEGRATIONS = (osm_access_integration)
PACKAGES = ('numpy','pandas','requests','urllib3','snowflake-snowpark-python')
HANDLER = 'retrieve'
AS $$

import requests
import json
import _snowflake

headers = ({'User-Agent':
            'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36'})





# Define a function to send the query and retrieve data
def retrieve():
    
    # Define the Overpass API URL
    overpass_url = "https://overpass-api.de/api/interpreter"

    # Define the query to retrieve hospitals in Porto Alegre
    query = """
    [out:json];
    area["name"="Porto Alegre"]->.searchArea;
    (
    node["amenity"="hospital"](area.searchArea);
    way["amenity"="hospital"](area.searchArea);
    relation["amenity"="hospital"](area.searchArea);
    );
    out center;
    """

    response = requests.get(overpass_url, params={'data': query})
    data = response.json()

    return data
    
$$;

-----Example
select retrieve_hospitals();