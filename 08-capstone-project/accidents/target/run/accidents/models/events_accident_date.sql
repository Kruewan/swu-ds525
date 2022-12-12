

  create view "dev"."public"."events_accident_date__dbt_tmp" as (
    select 
	count (*) , accident_date 
from "dev"."public"."stg_accidents"
group by  accident_date
order by  accident_date  desc
  ) ;
