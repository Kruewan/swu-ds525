select 
    accident_date , 
	to_date(accident_date, 'YYYYMMDD', FALSE) AS accident_date_new  ,
    accident_time ,
    expw_step ,
    weather_state ,
    injur_man ,
    injur_femel ,
    dead_man ,
    dead_femel ,
    cause 
from accidents