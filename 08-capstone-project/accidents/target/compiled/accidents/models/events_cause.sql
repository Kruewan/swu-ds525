select 
	count (*) , cause 
from "dev"."public"."stg_accidents"
group by  cause