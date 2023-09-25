CREATE OR REPLACE FUNCTION holidays_calendar(min_date varchar, max_date varchar)
RETURNS array
LANGUAGE PYTHON
RUNTIME_VERSION = 3.9
PACKAGES = ('requests','snowflake-snowpark-python','holidays')
HANDLER = 'get_holidays'
AS $$

import requests
import json
import _snowflake
import holidays

headers = ({'User-Agent':
            'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36'})

def get_holidays(min_date,max_date):


    holidays_list = []
    holidays_range= holidays.Brazil()
    for holiday in holidays_range[min_date:max_date] :
        holidays_list.append(holiday)

    return holidays_list
    
$$;



CREATE OR REPLACE TABLE calendar_holidays (
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
          FROM TABLE(GENERATOR(ROWCOUNT=>2000))),

      HOLIDAYS as (select f.value from TABLE(FLATTEN(holidays_calendar('2022-01-01','2024-01-01'))) f)

      SELECT MY_DATE
            ,YEAR(MY_DATE)
            ,MONTH(MY_DATE)
            ,MONTHNAME(MY_DATE)
            ,DAY(MY_DATE)
            ,DAYOFWEEK(MY_DATE)
            ,WEEKOFYEAR(MY_DATE)
            ,DAYOFYEAR(MY_DATE)
            ,HOLIDAYS.value
        FROM CTE_MY_DATE
        left join HOLIDAYS on CTE_MY_DATE.MY_DATE = HOLIDAYS.value
    ;

SELECT * FROM calendar_holidays;
