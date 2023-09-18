with employees_experience as (  
    select    
        {{ dbt_utils.generate_surrogate_key([
        'employee_urn',
        'title',
        'company_name',
        'start_date'
         ]) }} as employee_experience_id,        
        employee_urn,
        title,
        industry,	
        company_name,	
        is_target_company,		
        start_date_month,			
        start_date_year,			
        start_date,		
        end_date_month,		
        end_date_year,		
        end_date,			
        scraped_at,
        row_number() over(partition by employee_urn, title, company_name order by scraped_at desc) rn  
    from {{ source('raw_linkedinapi', 'employees_experience') }}
),

employees_experience_last_updated as (
  select 
      * except (rn),
      if(end_date is null,
        date_diff(date(scraped_at), start_date, month),
        date_diff(end_date, start_date, month)
      ) as months_in_company,
      if(end_date is null, true, false) as is_current_role,
      regexp_contains(lower(title), r'analyst|engineer|baz|data|machine learning|developer|architect') as is_role_in_target_list,
      regexp_contains(lower(title), r'internship') as is_internship
  from employees_experience 
  where rn = 1
)

select *
from employees_experience_last_updated
