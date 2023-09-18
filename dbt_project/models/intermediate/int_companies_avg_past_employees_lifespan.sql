with past_employees_total_months_in_company as (
  select
    employee_urn, 
    company_name, 
    is_target_company,
    is_role_in_target_list as are_roles_in_target_list,
    sum(months_in_company) as total_months_in_company,
    count(title) as number_of_roles
  from {{ ref('stg_linkedinapi__employees_experience_last_updated') }}
  where is_current_role is false
  and is_internship is false
  group by employee_urn, company_name, is_target_company, are_roles_in_target_list

),

past_employees_target_company as (
  select
    employee_urn,
    are_roles_in_target_list,
    company_name as target_company_name,
    sum(number_of_roles) as number_of_roles
  from past_employees_total_months_in_company
  where is_target_company is true
  group by employee_urn, are_roles_in_target_list, company_name
),

past_employees_avg_months_in_company as (
  select
    employee_urn,
    are_roles_in_target_list,
    avg(if(is_target_company is true, total_months_in_company, null)) as avg_months_in_target_company,
    avg(if(is_target_company is false, total_months_in_company, null)) as avg_months_in_other_companies
  from past_employees_total_months_in_company
  group by employee_urn, are_roles_in_target_list
),

join_past_employees_data as (
  select
    petc.employee_urn,
    petc.are_roles_in_target_list,
    petc.target_company_name,
    peam.avg_months_in_target_company,
    peam.avg_months_in_other_companies,
    petc.number_of_roles,
    round(peam.avg_months_in_target_company / peam.avg_months_in_other_companies,2) as ratio_months_in_target_company_vs_others
  from past_employees_target_company petc
  inner join past_employees_avg_months_in_company peam
  on (petc.employee_urn = peam.employee_urn
  and petc.are_roles_in_target_list = peam.are_roles_in_target_list)
),

companies_avg_ratio as (
  select
    {{ dbt_utils.generate_surrogate_key([
        'target_company_name',
        'are_roles_in_target_list'
    ]) }} as company_grouped_by_roles_id,
    target_company_name,
    are_roles_in_target_list,
    avg(avg_months_in_target_company) as avg_months_in_target_company,
    avg(avg_months_in_other_companies) as avg_months_in_other_companies,
    avg(ratio_months_in_target_company_vs_others) as avg_ratio_months_in_target_company_vs_others,
    sum(number_of_roles) as number_of_roles
  from join_past_employees_data
  group by target_company_name, are_roles_in_target_list
)


select * from companies_avg_ratio
