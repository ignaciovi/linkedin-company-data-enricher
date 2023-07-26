#!/usr/bin/env python
# coding: utf-8

# # Get company URN -> Get employees -> Get employee profile

# Remember to reload the library every time I make a change on the linkedinapi

# # Login

# In[ ]:


from linkedinapi.linkedin_api import Linkedin
from linkedinapi.linkedin_api.utils import helpers
from time import sleep
from google.cloud import storage
from hashlib import sha256

import datetime
import calendar
import json
import pandas as pd
import numpy as np
import os
import random


# In[ ]:


with open('credentials.json') as f:
    data = json.load(f)
    username = data['otherusername']
    password = data['otherpassword']

# Authenticate using any Linkedin account credentials
api = Linkedin(username, password)


# In[ ]:


os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="./storage-service-account.json"


# In[ ]:


storage_client = storage.Client()
bucket = storage_client.get_bucket("job-hunt-company-analyser")


# ## Method to get array of seconds between iterations

# In[ ]:


def subtract_adjacent_elements(arr):
    result = []
    for i in range(len(arr) - 1):
        result.append(arr[i+1] - arr[i])
    return result


# In[ ]:


def get_random_seconds_between_iterations_array(number_of_iterations, number_of_hours_for_job):
    number_of_seconds = number_of_hours_for_job * 60 * 60
    seconds_array = sorted(random.sample(range(1, number_of_seconds), number_of_iterations))
    
    print(seconds_array)
    seconds_diff_array = [0] + (subtract_adjacent_elements(seconds_array))
    
    return seconds_diff_array
    


# In[ ]:





# # Get company based on name

# In[ ]:





# In[ ]:


company_to_look = 'Odilo'


# In[ ]:


companies = api.get_companies(company_to_look)
today_date = datetime.datetime.now()
today = today_date.strftime("%Y-%m-%d %H:%M:%S")


# In[ ]:


pd.DataFrame(companies)


# # Select company and store it

# In[ ]:


searched_company = companies[0]


# In[ ]:


searched_company


# In[ ]:


# searched_company_urn = searched_company['entityUrn'].split(':')[-1].split(',')[0]
searched_company_urn = helpers.get_id_from_urn(searched_company['trackingUrn'])
searched_company_title = searched_company['title']
searched_company_industry = searched_company['primarySubtitle'].split(' • ')[0]
searched_company_location = searched_company['primarySubtitle'].split(' • ')[1]


# In[ ]:


id_to_encode = searched_company_urn + today

id_encoded = sha256(id_to_encode.encode('utf-8')).hexdigest()

searched_company_dict = {
    'id': id_encoded,
    'urn': searched_company_urn,
    'name': searched_company_title,
    'industry': searched_company_industry,
    'location': searched_company_location,
    'scraped_at': today,
}


# In[ ]:


df_company_searched = pd.DataFrame([searched_company_dict])


# In[ ]:


df_company_searched


# In[ ]:


# 76334424


# ## Store to bucket

# In[ ]:


file_name_company_name = searched_company_title.replace(' ', '_').lower()
file_name_date = today_date.strftime('%Y%m%d%H%M%S')


# In[ ]:


file_name_company_searched = f'company_searched_{file_name_company_name}_{file_name_date}'


# In[ ]:


file_name_company_searched


# In[ ]:


bucket.blob(f'company_searched/{file_name_company_searched}.csv').upload_from_string(
                    df_company_searched.to_csv(index=False), 'text/csv')


# # Get employees

# In[ ]:


employees_experience_input = {
    'role_keywords_searched': ['data', 'engineer', 'analyst', 'software', 'developer', 'fullstack', 
                          'full stack', 'full-stack', 'backend'],
    'threshold_number_of_roles_with_keywords_to_stop': 50,
    'threshold_total_number_of_roles_to_stop': 200,
    'threshold_number_of_iterations_to_stop': 20
}


# In[ ]:


# Find 50 employees that are not out of network

def get_employees_experience(employees_experience_input):
    employees_all_list = []
    employees_data_list = []
    
    seconds_diff_array = get_random_seconds_between_iterations_array(5, 1)

    iterator = 0
    while True:
        sleeptime = random.uniform(30, 90)

        selected_offset = 50 * iterator
        print(f'Offset: {selected_offset}')
        iterator += 1

        employees = api.get_employees(searched_company_urn, offset=selected_offset, count=50)

        if not bool(employees):
            break
      
        indexes_to_drop = []
        indexes_of_data = []

        for idx, x in enumerate(employees):
            if x['memberDistance'] == 'OUT_OF_NETWORK':
                indexes_to_drop += [idx]
            elif any([y in (x['primarySubtitle']).lower() for y in role_keywords_searched]):
                indexes_of_data += [idx]

        employees = np.array(employees)

        employees_data = employees[indexes_of_data]

        employees_all = np.delete(employees, indexes_to_drop)

        employees_all_list += list(employees_all)
        employees_data_list += list(employees_data)

        employees_all_list = list({v['entityUrn']:v for v in employees_all_list}.values())
        employees_data_list = list({v['entityUrn']:v for v in employees_data_list}.values())

        # Find at least X data employees and Y total employees
        # Set a maximum of iterations

        is_max_iterations = employees_experience_input['threshold_number_of_iterations_to_stop'] == iterator
        
        is_over_threshold_keyword_employees = len(employees_data_list) > employees_experience_input['threshold_number_of_roles_with_keywords_to_stop']
        is_over_threshold_all_employees = len(employees_all_list) > employees_experience_input['threshold_total_number_of_roles_to_stop']

        is_final_iteration = is_max_iterations or (is_over_threshold_keyword_employees and is_over_threshold_all_employees)

        if is_final_iteration:
            break

        sleep(sleeptime)
        
    return employees_all_list

    


# In[ ]:


# employees_data_list


# In[ ]:


# employees_all_list = employees_all_list[0:50]


# In[ ]:


employees_all_list = get_employees_experience(employees_experience_input)


# In[ ]:


pd.DataFrame(employees_all_list)


# In[ ]:





# # Get profile and experience details

# In[ ]:


len(employees_all_list)


# In[ ]:


# employees['elements'][1]['items'][6]['item']['entityResult']['image']['attributes'][0]['detailData']


# In[ ]:


# api.get_profile(urn_id='ACoAABAnHTkBUKK4tTdN_vwDDPW0IwLKI7tHIbE')


# In[ ]:


employees_experience = []
i = 0
            
today_date = datetime.datetime.now()
today = today_date.strftime("%Y-%m-%d %H:%M:%S")

for employee in employees_all_list:
    
    sleeptime = random.uniform(30, 90)
    urn = employee['entityUrn'].split(':')[-1].split(',')[0]
    
    i += 1
    
    if i%10 == 0:
        print(i)

    profile = api.get_profile(urn_id = urn)
    
    
    if not bool(profile):
        continue
        
#     for education in profile['education']:
        
    
    for experience in profile['experience']:
        
        try:
            
            is_intern = True if 'intern' in (experience['title']).lower() else False
            
            
            if is_intern:
                continue

            time_period = experience['timePeriod']

            start_date_month = int(time_period['startDate']['month'])
            start_date_year = int(time_period['startDate']['year'])
            start_date = datetime.date(start_date_year, start_date_month, 1)

            is_current_position = 'endDate' not in time_period

            if is_current_position:
                last_reference_year = None
                last_reference_month = None

                last_reference_date = today_date.date()

            else:
                last_reference_year = int(time_period['endDate']['year'])
                last_reference_month = int(time_period['endDate']['month'])
                last_reference_day = calendar.monthrange(last_reference_year, last_reference_month)[1]

                last_reference_date = datetime.date(last_reference_year, last_reference_month, last_reference_day)

            months_in_company = 12 * (last_reference_date.year - start_date.year) + (last_reference_date.month - start_date.month)

            end_date_year = None if is_current_position else last_reference_year
            end_date_month = None if is_current_position else last_reference_month
            end_date = None if is_current_position else last_reference_date
            
            is_target_company = experience['companyName'] in searched_company_title
            
            id_to_encode = urn + experience['title'] + experience['companyName'] + today
        
            id_encoded = sha256(id_to_encode.encode('utf-8')).hexdigest()
            
            employees_experience.append({
                'id': id_encoded,
                'employee_urn': urn,
                'title': experience['title'], 
                'industry': experience['company']['industries'][0],
                'company_name': experience['companyName'], 
                'is_current_position': is_current_position,
                'is_target_company': is_target_company,
                'start_date_month': start_date_month, 
                'start_date_year': start_date_year, 
                'start_date': start_date, 
                'end_date_month': end_date_month, 
                'end_date_year': end_date_year, 
                'end_date': end_date, 
                'months_in_company': months_in_company, 
                'scraped_at': today

            })

        except:
            continue

    sleep(sleeptime)
    


# In[ ]:


employees_experience


# In[ ]:


# TODO add education
# profile['education']


# In[ ]:


# employees_experience


# In[ ]:


df_employees_experience = pd.DataFrame(employees_experience)


# In[ ]:


df_employees_experience


# In[ ]:


# df_employees_experience[df_employees_experience['employee_urn'] == 'ACoAAB3ksgQBLsTVCfaFSdCGdVMzqVJHxuCDQy4']


# In[ ]:


# profile = api.get_profile(urn_id = 'ACoAAAPt8PQBiNyyY_-0Lcr198R9zbGOAOPdHwI')


# In[ ]:


# profile['education']


# In[ ]:





# In[ ]:


# df_employees_experience = df_employees_experience.rename(columns={'urn': 'employee_urn'})


# In[ ]:


df_employees_experience.query(f'company_name == \'{searched_company_title}\' and end_date == end_date')


# In[ ]:


group_months_in_company = df_employees_experience.query(f'company_name == \'{searched_company_title}\' and end_date == end_date').groupby('months_in_company')['months_in_company'].count()


# In[ ]:


group_months_in_company.sum()


# In[ ]:


df_group_months_in_company = pd.DataFrame(group_months_in_company).rename(columns={'months_in_company': 'count'}).reset_index()


# In[ ]:


df_group_months_in_company.query('months_in_company <= 12')['count'].sum()


# In[ ]:





# In[ ]:





# In[ ]:


# df = pd.read_pickle('employees_forth_point_20230721.pickle')


# In[ ]:


# df


# In[ ]:


file_name_employees_experience = f'employees_experience_{file_name_company_name}_{file_name_date}'


# In[ ]:


file_name_employees_experience


# In[ ]:


df_employees.to_pickle(f'{file_name}.pickle')


# ## Store in bucket

# In[ ]:


bucket.blob(f'employees_experience/{file_name_employees_experience}.csv').upload_from_string(
                    df_employees_experience.to_csv(index=False), 'text/csv')


# In[ ]:




