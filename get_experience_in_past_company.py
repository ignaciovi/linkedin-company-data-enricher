import datetime
import calendar
import json
import pandas as pd
import numpy as np
import os
import random

# TODO change to original Linkedin but not that it wont work unlesss pointing to branch
from linkedin_api import Linkedin
from linkedin_api.utils import helpers
from time import sleep
from google.cloud import storage
from hashlib import sha256

## TODO Do we need to wait for not being baned?

class Linkedin_Company_Data_Enricher(object):
    """
    Class for accessing Company Data Enricher functions
    """


    def __init__(
        self,
        input_parameters
    ):
        with open('credentials.json') as f:
            data = json.load(f)
            username = data['joeusername']
            password = data['joepassword']
        
        self.input_parameters = input_parameters
        self.api = Linkedin(username, password)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="./storage-service-account.json"
        today_datetime = datetime.datetime.now()
        self.today_datetime = today_datetime
        self.today_datetime_stringified = today_datetime.strftime("%Y-%m-%dT%H:%M:%S")
        self.today_datetime_timestamp = int(today_datetime.timestamp())

        GCP_BUCKET_NAME = 'job-hunt-company-analyser'

        storage_client = storage.Client()
        self.bucket = storage_client.get_bucket(GCP_BUCKET_NAME)

    def map_company(self, searched_company):
        searched_company_urn = searched_company['urn_id']
        searched_company_title = searched_company['name']
        searched_company_industry = searched_company['headline'].split(' • ')[0]
        searched_company_location = searched_company['headline'].split(' • ')[1]

        id_to_encode = searched_company_urn + str(self.today_datetime_timestamp)

        id_encoded = sha256(id_to_encode.encode('utf-8')).hexdigest()

        searched_company_dict = {
            'id': id_encoded,
            'urn': searched_company_urn,
            'name': searched_company_title,
            'industry': searched_company_industry,
            'location': searched_company_location,
            'scraped_at': self.today_datetime_stringified,
        }

        return searched_company_dict

    def store_search_log(self, input_parameters):


        search_log_dict = self.map_company(input_parameters['company'])

        search_log_dict['search_filters'] = input_parameters['search_filters']

        file_name_company_name = search_log_dict['name'].replace(' ', '_').lower()
        file_name_date = str(self.today_datetime_timestamp)
        file_name_search_log = f'company_searched_{file_name_company_name}_{file_name_date}'

        df_search_log = pd.DataFrame([search_log_dict])

        self.bucket.blob(f'company_searched/{file_name_search_log}.csv').upload_from_string(
                            df_search_log.to_csv(index=False), 'text/csv')

    # Split in past and present because we want a few of each
    def get_past_employees(self, searched_company_urn, limit):

        employees = self.api.search_people(past_companies=[searched_company_urn], limit=limit)

        return employees

    def get_present_employees(self, searched_company_urn, limit):

        employees = self.api.search_people(current_company=[searched_company_urn], limit=limit)

        return employees

    def get_employees_experience(self, employees_all_list, searched_company_title):

        employees_experience = []
        for employee in employees_all_list:
    
            urn = employee['urn_id']
            
            profile = self.api.get_profile(urn_id = urn)
            
            if not bool(profile):
                continue

            for experience in profile['experience']:

                try:

                    time_period = experience['timePeriod']

                    start_date_month = int(time_period['startDate']['month'])
                    start_date_year = int(time_period['startDate']['year'])
                    start_date = datetime.date(start_date_year, start_date_month, 1)

                    is_current_position = 'endDate' not in time_period

                    end_date_year = None if is_current_position else int(time_period['endDate']['year'])
                    end_date_month = None if is_current_position else int(time_period['endDate']['month'])
                    end_date_day = None if is_current_position else calendar.monthrange(end_date_year, end_date_month)[1]
                    end_date = None if is_current_position else datetime.date(end_date_year, end_date_month, end_date_day)
                    
                    start_date_stringified = None if start_date is None else start_date.strftime("%Y-%m-%d")
                    end_date_stringified = None if end_date is None else end_date.strftime("%Y-%m-%d")

                    is_target_company = experience['companyName'] in searched_company_title
                    
                    id_to_encode = urn + experience['title'] + experience['companyName'] + str(self.today_datetime_timestamp)
                
                    id_encoded = sha256(id_to_encode.encode('utf-8')).hexdigest()
                    
                    employees_experience.append({
                        'id': id_encoded,
                        'employee_urn': urn,
                        'title': experience['title'], 
                        'industry': None if 'industries' not in str(experience) else experience['company']['industries'][0],
                        'company_name': experience['companyName'], 
                        'is_current_position': is_current_position,
                        'is_target_company': is_target_company,
                        'start_date_month': start_date_month, 
                        'start_date_year': start_date_year, 
                        'start_date': start_date_stringified, 
                        'end_date_month': end_date_month, 
                        'end_date_year': end_date_year, 
                        'end_date': end_date_stringified, 
                        'scraped_at': self.today_datetime_stringified

                    })
                
                except:
                    print(urn)
                    break

        return employees_experience
    
    def store_experience(self, employees_experience, searched_company_title):
        
        file_name_company_name = searched_company_title.replace(' ', '_').lower()
        file_name_date = self.today_datetime_timestamp
        file_name_employees_experience = f'employees_experience_{file_name_company_name}_{file_name_date}'

        df_employees_experience = pd.DataFrame(employees_experience)

        # TODO replace with AWS
        self.bucket.blob(f'employees_experience/{file_name_employees_experience}.csv').upload_from_string(
                    df_employees_experience.to_csv(index=False), 'text/csv')

    def run(self, input_parameters):

        self.store_search_log(input_parameters)

        company_urn = input_parameters['company']['urn_id']
        company_name = input_parameters['company']['name']

        past_employees_list = self.get_past_employees(company_urn, 1)

        present_employees_list = self.get_present_employees(company_urn, 1)

        employees = past_employees_list + present_employees_list

        experience = self.get_employees_experience(employees, company_name)

        self.store_experience(experience, company_name)
