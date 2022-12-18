select 
	accident_date  ,
    accident_time ,
    expw_step ,
    weather_state ,
    injur_man ,
    injur_femel ,
    dead_man ,
    dead_femel ,
    cause 
from {{ ref('stg_accident_2559') }}

union

select 
	accident_date  ,
    accident_time ,
    expw_step ,
    weather_state ,
    injur_man ,
    injur_femel ,
    dead_man ,
    dead_femel ,
    cause 
from {{ ref('stg_accident_2560') }}

union

select 
	accident_date  ,
    accident_time ,
    expw_step ,
    weather_state ,
    injur_man ,
    injur_femel ,
    dead_man ,
    dead_femel ,
    cause 
from {{ ref('stg_accident_2561') }}

union

select 
	accident_date  ,
    accident_time ,
    expw_step ,
    weather_state ,
    injur_man ,
    injur_femel ,
    dead_man ,
    dead_femel ,
    cause 
from {{ ref('stg_accident_2562') }}

union

select 
	accident_date  ,
    accident_time ,
    expw_step ,
    weather_state ,
    injur_man ,
    injur_femel ,
    dead_man ,
    dead_femel ,
    cause 
from {{ ref('stg_accident_2563') }}

union

select 
	accident_date  ,
    accident_time ,
    expw_step ,
    weather_state ,
    injur_man ,
    injur_femel ,
    dead_man ,
    dead_femel ,
    cause 
from {{ ref('stg_accident_2564') }}

union

select 
	accident_date  ,
    accident_time ,
    expw_step ,
    weather_state ,
    injur_man ,
    injur_femel ,
    dead_man ,
    dead_femel ,
    cause 
from {{ ref('stg_accident_2565') }}

