with source as (

    select * from {{ source('raw', 'raw_customers') }}

),

renamed as (

    select
        customer_id,
        customer_name,
        lower(trim(plan_tier))          as plan_tier,
        cast(signup_date as date)       as signup_date,
        cast(monthly_revenue as double) as monthly_revenue,
        cast(is_active as boolean)      as is_active

    from source

)

select * from renamed