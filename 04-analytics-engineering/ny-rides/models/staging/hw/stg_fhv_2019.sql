{{
    config(
        materialized='view'
    )
}}

with fhv as (

    select *
    -- row_number() over(partition by dispatching_base_num, pickup_datetime) as rn
    from {{ source('hw', 'fhv_2019') }}
    where dispatching_base_num is not null
)

select
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
    dispatching_base_num as dispatching_base_num,
    {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }} as dropoff_locationid,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,

    -- shared ride
    coalesce({{ dbt.safe_cast("SR_Flag", api.Column.translate_type("integer")) }},0) as sharedride_flag

from fhv

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}