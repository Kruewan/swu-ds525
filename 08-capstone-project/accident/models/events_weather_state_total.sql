select 
	accident_date , weather_state 
from {{ ref('stg_accidents') }}

