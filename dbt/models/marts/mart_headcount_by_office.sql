select
    office,
    count(*) as employee_count,
    count(distinct job_title) as distinct_job_titles,
    min(transformed_at) as first_seen_at,
    max(transformed_at) as last_refreshed_at
from {{ ref('dim_employees') }}
group by office
order by employee_count desc
