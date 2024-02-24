{{
    config(
        materialized='view'
    )
}}

with tripdata as (

    select * from {{ source('staging', 'fhv_tripdata') }}

),

renamed as (

    select
        index_,
        dispatching_base_num,
        pickup_datetime,
        dropoff_datetime,
    {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }} as dropoff_locationid,
        sr_flag,
        affiliated_base_number

    from tripdata

)

select * from renamed where extract(year from pickup_datetime) = 2019

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}