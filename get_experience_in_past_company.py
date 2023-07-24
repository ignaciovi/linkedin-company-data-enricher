#!/usr/bin/env python
# coding: utf-8

# # Get company URN -> Get employees -> Get employee profile

# Remember to reload the library every time I make a change on the linkedinapi

# # Login

# In[ ]:


from linkedinapi.linkedin_api import Linkedin
from linkedinapi.linkedin_api.utils import helpers
from numpy import random
from time import sleep
from google.cloud import storage


import datetime
import calendar
import json
import pandas as pd
import numpy as np
import os


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


# # Get company based on name

# In[ ]:


company_to_look = 'Too good to go'


# In[ ]:


companies = api.get_companies(company_to_look)
today = datetime.date.today()


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


searched_company_dict = {
    'company_urn': searched_company_urn,
    'company_title': searched_company_title,
    'company_industry': searched_company_industry,
    'company_location': searched_company_location,
    'scraped_at': today,
}


# In[ ]:


pd.DataFrame([searched_company_dict])


# In[ ]:


# 76334424


# In[ ]:


file_name_company_name = searched_company_title.replace(' ', '_').lower()
file_name_date = today.strftime('%Y%m%d')


# In[ ]:


file_name_company_searched = f'company_searched_{file_name_company_name}_{file_name_date}'


# In[ ]:


bucket.blob(f'company_searched/{file_name_company_searched}.csv').upload_from_string(
                    df_employees.to_csv(index=False), 'text/csv')


# # Get employees

# In[ ]:


# TODO increase number of employees displayed playing with offset and count
# employees = api.get_employees(searched_company_urn, offset=1, count=2)
# employees = api.get_employees(3266, count=5)


# In[ ]:


# employees


# In[ ]:


# Find 50 employees that are not out of network

employees_list = []
for selected_offset in [0,50,100,150,200,250]:
    print(selected_offset)
    employees = api.get_employees(searched_company_urn, offset=selected_offset, count=50)
    indexes_to_drop = []
    
    for idx, x in enumerate(employees):
        if x['memberDistance'] == 'OUT_OF_NETWORK':
            indexes_to_drop += [idx]
    
    employees = np.delete(employees, indexes_to_drop)
    
    employees_list += list(employees)
    
    employees_list = list({v['entityUrn']:v for v in employees_list}.values())
    
    if len(employees_list) >= 50:
        break
    


# In[ ]:


employees_list = employees_list[0:50]


# In[ ]:


pd.DataFrame(employees_list)


# In[ ]:





# # Get profile and experience details

# In[ ]:


len(employees_list)


# In[ ]:


# employees['elements'][1]['items'][6]['item']['entityResult']['image']['attributes'][0]['detailData']


# In[ ]:


# api.get_profile(urn_id='ACoAABAnHTkBUKK4tTdN_vwDDPW0IwLKI7tHIbE')


# In[ ]:


employees_experience = []
i = 0
for employee in employees_list:
    sleeptime = random.uniform(2, 15)
    urn = employee['entityUrn'].split(':')[-1].split(',')[0]
    
    i += 1
    
    if i%10 == 0:
        print(i)

    profile = api.get_profile(urn_id = urn)
    
    if not bool(profile):
        continue
    
    for experience in profile['experience']:
        
        try:
            
            today = datetime.date.today()

            time_period = experience['timePeriod']

            start_date_month = int(time_period['startDate']['month'])
            start_date_year = int(time_period['startDate']['year'])
            start_date = datetime.date(start_date_year, start_date_month, 1)

            is_current_position = 'endDate' not in time_period

            if is_current_position:
                last_reference_year = None
                last_reference_month = None

                last_reference_date = today

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
            
            employees_experience.append({
                'urn': urn,
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


df_employees = pd.DataFrame(employees_experience)


# In[ ]:


df_employees.head()


# In[ ]:





# In[ ]:





# In[ ]:


df_employees.query(f'company_name == \'{searched_company_title}\' and end_date == end_date').head()


# In[ ]:


group_months_in_company = df_employees.query(f'company_name == \'{searched_company_title}\' and end_date == end_date').groupby('months_in_company')['months_in_company'].count()


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


# In[ ]:





# # Store in bucket

# In[ ]:


bucket.blob(f'employees_experience/{file_name_employees_experience}.csv').upload_from_string(
                    df_employees.to_csv(index=False), 'text/csv')


# In[ ]:




