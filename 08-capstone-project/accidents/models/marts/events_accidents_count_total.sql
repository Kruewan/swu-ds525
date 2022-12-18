select 
	accident_date  ,
    accident_time ,
    expw_step ,
    weather_state ,
    count(*) AS events_accidents_total ,
    cause 
from {{ ref('events_accidents_total') }}
group by   accident_date , accident_time , expw_step , weather_state , cause
