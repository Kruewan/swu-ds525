select 
	count (*) , accident_date 
from {{ ref('stg_accidents') }}
group by  accident_date
order by  accident_date  desc