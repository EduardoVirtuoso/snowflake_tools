CREATE OR REPLACE FUNCTION holidays_calendar(input_value varchar)
RETURNS array
LANGUAGE PYTHON
RUNTIME_VERSION = 3.9
--You need to create a access integration to access the api. 
--Check the documentation https://docs.snowflake.com/en/developer-guide/external-network-access
EXTERNAL_ACCESS_INTEGRATIONS = (tomtom_access_integration)
PACKAGES = ('numpy','pandas','requests','urllib3','snowflake-snowpark-python','holidays')
--You also need to create a secret with your api key. 
--Check the documentation https://docs.snowflake.com/en/sql-reference/sql/create-secret
SECRETS = ('tomtom_key'=tomtom_api_key)
HANDLER = 'geocode'
AS $$

import requests
import json
import _snowflake
import holidays

headers = ({'User-Agent':
            'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36'})

def geocode(input_value):


    lista_feriados = []
    feriados= holidays.Brazil()
    for feriado in feriados['2023-01-01': '2024-01-01'] :
        lista_feriados.append(feriado)

    return lista_feriados
    
$$;



CREATE OR REPLACE TABLE calendar_teste (
       MY_DATE          DATE        NOT NULL
      ,YEAR             SMALLINT    NOT NULL
      ,MONTH            SMALLINT    NOT NULL
      ,MONTH_NAME       CHAR(3)     NOT NULL
      ,DAY_OF_MON       SMALLINT    NOT NULL
      ,DAY_OF_WEEK      VARCHAR(9)  NOT NULL
      ,WEEK_OF_YEAR     SMALLINT    NOT NULL
      ,DAY_OF_YEAR      SMALLINT    NOT NULL
      ,FERIADOS         DATE        NULL
    )
    AS
      WITH CTE_MY_DATE AS (
        SELECT DATEADD(DAY, SEQ4(), '2022-01-01') AS MY_DATE
          FROM TABLE(GENERATOR(ROWCOUNT=>4000))),

      feriados as (select f.value from TABLE(FLATTEN(holidays_calendar('Arena do Grêmio, Porto Alegre, RS'))) f)

      SELECT MY_DATE
            ,YEAR(MY_DATE)
            ,MONTH(MY_DATE)
            ,MONTHNAME(MY_DATE)
            ,DAY(MY_DATE)
            ,DAYOFWEEK(MY_DATE)
            ,WEEKOFYEAR(MY_DATE)
            ,DAYOFYEAR(MY_DATE)
            ,feriados.value
        FROM CTE_MY_DATE
        left join feriados on CTE_MY_DATE.MY_DATE = feriados.value
    ;

SELECT * FROM calendar_teste;


with feriados as (
-----Example
select f.value from TABLE(FLATTEN(holidays_calendar('Arena do Grêmio, Porto Alegre, RS'))) f)


select * from calendar
left join feriados on calendar.MY_DATE = feriados.value
;