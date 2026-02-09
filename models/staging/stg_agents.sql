with source as (

    select * from {{ source('raw', 'raw_agents') }}

),

renamed as (

    select
        agent_id,
        agent_name,
        lower(trim(team))           as team,
        lower(trim(shift))          as shift,
        cast(hire_date as date)     as hire_date

    from source

)

select * from renamed