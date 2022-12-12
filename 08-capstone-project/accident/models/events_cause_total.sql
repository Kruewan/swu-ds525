select 
	accident_date , cause 
from {{ ref('stg_accidents') }}