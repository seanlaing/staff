select
    count(*) as total_employees,
    count(distinct office) as distinct_offices,
    count(distinct job_title) as distinct_job_titles,
    count(distinct address) as distinct_addresses,
    round(avg(length(phone_digits)), 1) as avg_phone_digits_length,
    max(transformed_at) as data_refreshed_at
from {{ ref('dim_employees') }}
