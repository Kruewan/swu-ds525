select 
	accident_date  ,
    accident_date_new ,
    accident_time ,
    expw_step ,
    weather_state ,
    injur_man ,
    injur_femel ,
    dead_man ,
    dead_femel ,
    cause 
from {{ ref('stg_accidents') }}
where accident_date  IS NOT NULL
and   accident_time  IS NOT NULL
and   cause	         IS NOT NULL
and   expw_step		 IS NOT NULL

