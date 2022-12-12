select 
	count (*) , accident_date , expw_step , cause
from {{ ref('stg_accidents') }}
group by  accident_date , expw_step , cause

