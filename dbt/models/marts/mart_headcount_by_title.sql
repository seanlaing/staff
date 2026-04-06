select
    job_title,
    count(*) as employee_count,
    count(distinct office) as office_count,
    min(transformed_at) as first_seen_at,
    max(transformed_at) as last_refreshed_at
from {{ ref('dim_employees') }}
group by job_title
order by employee_count desc
