with src as (
    select * from {{ source('raw', 'employees_raw') }}
),

cleaned as (
    select
        nullif(trim(name), '') as employee_name,
        nullif(trim(title), '') as job_title,
        nullif(trim(office), '') as office,
        nullif(trim(address), '') as address,
        nullif(
            regexp_replace(trim(coalesce(phone, '')), '[^0-9]', ''),
            ''
        ) as phone_digits,
        current_timestamp as transformed_at
    from src
)

select * from cleaned
