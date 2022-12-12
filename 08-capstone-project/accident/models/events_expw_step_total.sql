select 
	accident_date , expw_step 
from {{ ref('stg_accidents') }}