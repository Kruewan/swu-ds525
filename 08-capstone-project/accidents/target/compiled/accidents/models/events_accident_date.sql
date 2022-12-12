select 
	count (*) , accident_date 
from "dev"."public"."stg_accidents"
group by  accident_date
order by  accident_date  desc