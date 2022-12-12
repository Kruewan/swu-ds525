select 
	count (*) , expw_step 
from "dev"."public"."stg_accidents"
group by  expw_step