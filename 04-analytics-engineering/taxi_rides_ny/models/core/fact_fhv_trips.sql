{{
    config(
        materialized='table'
    )
}}

with fhv_tripdata as (
    select *,
        'Fhv' as service_type
    from {{ ref('stg_fhv_tripdata') }}
),

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select 
    fhv_tripdata.index_,
    fhv_tripdata.dispatching_base_num,
    fhv_tripdata.pickup_datetime,
    fhv_tripdata.dropoff_datetime,
    pickup_zone.zone as pickup_zone, 
    dropoff_zone.zone as dropoff_zone,  

from fhv_tripdata
inner join dim_zones as pickup_zone
on fhv_tripdata.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_tripdata.dropoff_locationid = dropoff_zone.locationid