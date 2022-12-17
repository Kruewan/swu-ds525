select 
	accident_date  ,
    accident_time ,
    expw_step ,
    weather_state ,
    injur_man ,
    injur_femel ,
    dead_man ,
    dead_femel ,
    cause 
from {{ ref('stg_accidents') }}
where accident_date  != '' 
and   accident_time  != '' 
and   cause	         != '' 
and   expw_step		 != '' 

