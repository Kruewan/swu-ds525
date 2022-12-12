select 
	count (*) , weather_state 
from {{ ref('stg_accidents') }}
group by  weather_state

