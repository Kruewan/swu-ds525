

  create view "dev"."public"."events_cause__dbt_tmp" as (
    select 
	count (*) , cause 
from "dev"."public"."stg_accidents"
group by  cause
  ) ;
