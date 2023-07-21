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

import datetime
import calendar
import json
import pandas as pd


# In[ ]:


with open('credentials.json') as f:
    data = json.load(f)
    username = data['otherusername']
    password = data['otherpassword']

# Authenticate using any Linkedin account credentials
api = Linkedin(username, password)


# # Get company based on name

# In[ ]:


companies = api.get_companies('Phlo')


# In[ ]:


companies


# In[ ]:


pd.DataFrame(companies)


# # Select company and store it

# In[ ]:


searched_company = companies[0]


# In[ ]:


searched_company['trackingUrn']


# In[ ]:


# searched_company_urn = searched_company['entityUrn'].split(':')[-1].split(',')[0]
searched_company_urn = helpers.get_id_from_urn(searched_company['trackingUrn'])
searched_company_title = searched_company['title']
searched_company_industry = companies[0]['primarySubtitle'].split(' • ')[0]
searched_company_location = companies[0]['primarySubtitle'].split(' • ')[1]


# In[ ]:


searched_company_dict = {
    'company_urn': searched_company_urn,
    'company_title': searched_company_title,
    'company_industry': searched_company_industry,
    'company_location': searched_company_location,
}


# In[ ]:


pd.DataFrame([searched_company_dict])


# In[ ]:


# 76334424


# # Get employees

# In[ ]:


# TODO increase number of employees displayed playing with offset and count
# employees = api.get_employees(searched_company_urn, offset=25, count=10)
employees = api.get_employees(3266, count=5)


# # Get profile and experience details

# In[ ]:


employees


# In[ ]:


len(employees)


# In[ ]:


# employees['elements'][1]['items'][6]['item']['entityResult']['image']['attributes'][0]['detailData']


# In[ ]:


# api.get_profile(urn_id='ACoAABAnHTkBUKK4tTdN_vwDDPW0IwLKI7tHIbE')


# In[ ]:


employees_experience = []
i = 0
for employee in employees:
    sleeptime = random.uniform(2, 15)
    urn = employee['entityUrn'].split(':')[-1].split(',')[0]
#     urn = helpers.get_id_from_urn(employee['trackingUrn'])
    
    if urn == 'headless':
        continue
    
    if employee['memberDistance'] == 'OUT_OF_NETWORK':
        continue
    
    i += 1
    
    print(i)
    print(urn)

    profile = api.get_profile(urn_id = urn)
    
    if not bool(profile):
        continue
    
    for experience in profile['experience']:
        
        try:
            
            today = datetime.date.today()
        #     print(experience)
    #         print(experience['title'])
    #         print(experience['company']['industries'])
    #         print(experience['companyName'])
            time_period = experience['timePeriod']
    #         print(time_period['startDate'])
    #         if 'endDate' in time_period:
    #             print(time_period['endDate'])

    #         is_company_searched = experience['companyName'] in searched_company_title
    #         is_past_company = 'endDate' in time_period

    #         if is_company_searched: 

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
            

            employees_experience.append({
                'urn': urn,
                'title': experience['title'], 
                'industry': experience['company']['industries'][0],
                'company_name': experience['companyName'], 
                'is_current_position': is_current_position,
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


df = pd.DataFrame(employees_experience)


# In[ ]:


df


# In[ ]:




