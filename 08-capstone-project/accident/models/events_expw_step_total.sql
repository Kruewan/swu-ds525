select 
	count (*) , expw_step 
from {{ ref('stg_accidents') }}
group by  expw_step