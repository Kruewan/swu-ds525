

  create view "dev"."public"."events_expw_step__dbt_tmp" as (
    select 
	count (*) , expw_step 
from "dev"."public"."stg_accidents"
group by  expw_step
  ) ;
