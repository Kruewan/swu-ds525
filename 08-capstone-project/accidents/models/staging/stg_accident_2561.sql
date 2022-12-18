select 
	to_date(accident_date, 'YYYYMMDD', FALSE) AS accident_date  ,
    accident_time ,
    expw_step ,
    weather_state ,
    injur_man ,
    injur_femel ,
    dead_man ,
    dead_femel ,
    cause 
from accident_2561