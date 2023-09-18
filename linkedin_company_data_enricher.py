""" 
    Linkedin Company Data Enricher
    Retrieves employees information from a given target company using LinkedinAPI
"""

import os
import datetime
import calendar
import json
from hashlib import sha256
import pandas as pd
from linkedin_api import Linkedin
from linkedin_api.utils import helpers
from google.cloud import storage


class LinkedinCompanyDataEnricher(object):
    """
    Class for accessing Company Data Enricher functions
    """

    def __init__(self, input_parameters):
        with open("credentials.json") as f:
            data = json.load(f)
            username = data["joeusername"]
            password = data["joepassword"]

        self.input_parameters = input_parameters
        self.api = Linkedin(username, password)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./storage-service-account.json"
        today_datetime = datetime.datetime.now()
        self.today_datetime_stringified = today_datetime.strftime("%Y-%m-%dT%H:%M:%S")
        self.today_datetime_timestamp = int(today_datetime.timestamp())

        gcp_bucket_name = "job-hunt-company-analyser"

        storage_client = storage.Client()
        self.bucket = storage_client.get_bucket(gcp_bucket_name)

    def map_company(self, searched_company):
        """
        Maps searched company to desired CSV output format
        """
        searched_company_urn = searched_company["urn_id"]
        searched_company_title = searched_company["name"]
        searched_company_industry = searched_company["headline"].split(" • ")[0]
        searched_company_location = searched_company["headline"].split(" • ")[1]

        id_to_encode = searched_company_urn + str(self.today_datetime_timestamp)

        id_encoded = sha256(id_to_encode.encode("utf-8")).hexdigest()

        searched_company_dict = {
            "id": id_encoded,
            "urn": searched_company_urn,
            "name": searched_company_title,
            "industry": searched_company_industry,
            "location": searched_company_location,
            "scraped_at": self.today_datetime_stringified,
        }

        return searched_company_dict

    def store_data(self, file_name, searched_company_title, data):
        """
        Stores data in CSV GCP bucket
        """
        file_name_company_name = searched_company_title.replace(" ", "_").lower()
        file_name_date = self.today_datetime_timestamp
        file_name_csv = f"{file_name}_{file_name_company_name}_{file_name_date}"

        df = pd.DataFrame(data)

        self.bucket.blob(f"{file_name}/{file_name_csv}.csv").upload_from_string(
            df.to_csv(index=False), "text/csv"
        )

    def store_search_log(self):
        """
        Stores search log CSV data into GCP
        """

        search_log_dict = self.map_company(self.input_parameters["company"])
        search_log_dict["search_filters"] = self.input_parameters["search_filters"]
        searched_company_title = self.input_parameters["company"]["name"]

        self.store_data("company_searched", searched_company_title, [search_log_dict])

    # Split in past and present because we want a few of each
    def get_past_employees(self, searched_company_urn, limit):
        """
        Retrieves past employees data for target company
        """
        employees = self.api.search_people(
            past_companies=[searched_company_urn], limit=limit
        )

        return employees

    def get_present_employees(self, searched_company_urn, limit):
        """
        Retrieves current employees data for target company
        """
        employees = self.api.search_people(
            current_company=[searched_company_urn], limit=limit
        )

        return employees

    def get_employees_experience(self, profile, searched_company_title):
        """
        Retrieve employees experience from employees profile
        """
        employees_experience = []
        urn = helpers.get_id_from_urn(profile["entityUrn"])

        for experience in profile["experience"]:
            try:
                time_period = experience["timePeriod"]

                start_date_month = int(time_period["startDate"]["month"])
                start_date_year = int(time_period["startDate"]["year"])
                start_date = datetime.date(start_date_year, start_date_month, 1)

                is_current_position = "endDate" not in time_period

                end_date_year = (
                    None if is_current_position else int(time_period["endDate"]["year"])
                )
                end_date_month = (
                    None
                    if is_current_position
                    else int(time_period["endDate"]["month"])
                )
                end_date_day = (
                    None
                    if is_current_position
                    else calendar.monthrange(end_date_year, end_date_month)[1]
                )
                end_date = (
                    None
                    if is_current_position
                    else datetime.date(end_date_year, end_date_month, end_date_day)
                )

                start_date_stringified = (
                    None if start_date is None else start_date.strftime("%Y-%m-%d")
                )
                end_date_stringified = (
                    None if end_date is None else end_date.strftime("%Y-%m-%d")
                )

                is_target_company = experience["companyName"] in searched_company_title

                id_to_encode = (
                    urn
                    + experience["title"]
                    + experience["companyName"]
                    + str(self.today_datetime_timestamp)
                )

                id_encoded = sha256(id_to_encode.encode("utf-8")).hexdigest()

                employees_experience.append(
                    {
                        "id": id_encoded,
                        "employee_urn": urn,
                        "title": experience["title"],
                        "industry": None
                        if "industries" not in str(experience)
                        else experience["company"]["industries"][0],
                        "company_name": experience["companyName"],
                        "is_target_company": is_target_company,
                        "start_date_month": start_date_month,
                        "start_date_year": start_date_year,
                        "start_date": start_date_stringified,
                        "end_date_month": end_date_month,
                        "end_date_year": end_date_year,
                        "end_date": end_date_stringified,
                        "scraped_at": self.today_datetime_stringified,
                    }
                )

            except:
                break

        return employees_experience

    def get_employees_education(self, profile):
        """
        Retrieve employees education from employees profile
        """
        employees_education = []

        urn = helpers.get_id_from_urn(profile["entityUrn"])

        for education in profile["education"]:
            try:
                school_name = education.get("schoolName")
                field_of_study = education.get("fieldOfStudy")
                start_date_year = education.get("timePeriod")["startDate"]["year"]
                end_date_year = education.get("timePeriod")["endDate"]["year"]

                id_to_encode = (
                    urn
                    + education["schoolName"]
                    + education["fieldOfStudy"]
                    + str(self.today_datetime_timestamp)
                )

                id_encoded = sha256(id_to_encode.encode("utf-8")).hexdigest()

                employees_education.append(
                    {
                        "id": id_encoded,
                        "employee_urn": urn,
                        "school_name": school_name,
                        "field_of_study": field_of_study,
                        "start_date_year": start_date_year,
                        "end_date_year": end_date_year,
                    }
                )

            except:
                break

        return employees_education

    def get_and_store_employees_data(self, employees_all_list, searched_company_title):
        """
        Get and store employees experience and education
        """
        employees_experience = []
        employees_education = []
        for employee in employees_all_list:
            urn = employee["urn_id"]

            profile = self.api.get_profile(urn_id=urn)

            if not bool(profile):
                continue

            employee_experience = self.get_employees_experience(
                profile, searched_company_title
            )
            employees_experience += employee_experience

            employee_education = self.get_employees_education(profile)
            employees_education += employee_education

        self.store_data(
            "employees_experience", searched_company_title, employees_experience
        )
        self.store_data(
            "employees_education", searched_company_title, employees_education
        )

    def run(self):
        """
        Run functions to get and store employees experience and education for target company
        """
        company_urn = self.input_parameters["company"]["urn_id"]
        company_name = self.input_parameters["company"]["name"]
        limit = self.input_parameters["search_filters"]["limit"]

        self.store_search_log()

        past_employees_list = self.get_past_employees(company_urn, limit)

        present_employees_list = self.get_present_employees(company_urn, limit)

        employees = past_employees_list + present_employees_list

        self.get_and_store_employees_data(employees, company_name)
