create or replace function tomtom_table_geocoder(end varchar)
returns table (end varchar,result variant)
language python
runtime_version=3.9
--You need to create a access integration to access the api. 
--Check the documentation https://docs.snowflake.com/en/developer-guide/external-network-access
EXTERNAL_ACCESS_INTEGRATIONS = (tomtom_access_integration)
PACKAGES = ('numpy','pandas','requests','urllib3','snowflake-snowpark-python')
--You also need to create a secret with your api key. 
--Check the documentation https://docs.snowflake.com/en/sql-reference/sql/create-secret
SECRETS = ('tomtom_key'=tomtom_api_key)
handler='geocode'
as $$

import requests
import json
import _snowflake

headers = ({'User-Agent':
            'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36'})

class geocode:

    def process(self, end):

        #Create a variable with the secret
        key = _snowflake.get_generic_secret_string('tomtom_key')

        #get address
        url_tomtom = f'https://api.tomtom.com/search/2/geocode/{end}.json?key={key}'
        r = requests.get(url_tomtom, headers=headers)
        self.end = r.json()
        yield (end,self.end)
    
    

$$;

---Example
CREATE or replace TEMPORARY TABLE adresses(endereco varchar);


INSERT INTO adresses (endereco)
   VALUES ('Largo dos Campeões, Porto Alegre, RS');

   
select * from adresses,table(tomtom_table_geocoder(endereco));
