select 
	count (*) , cause 
from {{ ref('stg_accidents') }}
group by  cause