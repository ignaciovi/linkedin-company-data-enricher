"""
    Example of triggering LinkedinCompanyDataEnricher by first searching for target company URN
"""

import json
import pandas as pd
from linkedin_api import Linkedin
from linkedin_company_data_enricher import LinkedinCompanyDataEnricher

with open("credentials.json") as f:
    data = json.load(f)
    username = data["joeusername"]
    password = data["joepassword"]

TARGET_COMPANY_NAME = "Google"
search_company_api = Linkedin(username, password)

companies = search_company_api.search_companies(keywords=TARGET_COMPANY_NAME, limit=10)
pd.DataFrame(companies)

# Select target compnay from output list
searched_company = companies[0]

search_input = {
    "limit": 100,
}

input_to_send = {"company": searched_company, "search_filters": search_input}

api = LinkedinCompanyDataEnricher(input_to_send)
api.run()
