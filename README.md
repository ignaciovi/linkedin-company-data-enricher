# Linkedin Company Data Enricher

> This project attempts to infer the quality of an organization's culture based on the employee role rotation. I.e. how long do employees stay at the company and is this an indicator of how good a company is to work for? For this, I used an Open Source Linkedin scraper to retrieve employees portfolio for target companies and applied data modelling to calculate the metrics that would help me determine the quality of the target company's culture. The API didn't work at the first since Linkedin had just changed their endpoint, so I fixed it and merged a PR, helping maintain the Open Source library


## Motivation
One of the biggest uncertainties about looking for a new job is finding out if the company you are applying for has a good Organizational Culture. We define Organizational Culture as the set of values, beliefs, attitudes, systems, and rules that outline and influence employee behavior within the organization.

Personally, on my job hunting process I retrieve three types of information in order to determine if a company is worth applying for:
1. Does the role description fit my experience and expectations?
2. Do I like what the company industry? Do I like the type of projects they do?
3. What is the overall reputation of the company? (I check on Glassdoor and other review places like Trustpilot/Google if they offer a product that can be scored)

However, I always found that there was a missing piece of information. I wanted to have more information about the Organization's culture. Most of the people find some cues about the culture on the interviewer's behaviour but, is there any other more objective way to find out about this?

From experience and from people I'm close with, people tend to not stay for long in a company if it has a toxic environment or has bad work/life balance, i.e. bad culture. This made me wonder, can I infer a company's culture based on how long do they stay in the company and how long compared to the average they have stayed in previous companies?


## Observations
I have to note that even though employees rotation is a useful metric to better understand a company's culture, it might not be very reliable. There are a few reasons:
- Older employees tend to be less prone to switch companies (can we prove this assumption?)
- Employees might not change jobs because they can't find anything better on the market or the situation of the market is unstable to find another job
- Each role has different types of employee profile and behaviour. For instance, young software engineerings tend to stay in a company 2-3 years independently of the quality of the company (can we prove this assumption?)
- Most contractors stay for a defined lenght in the company. We can't tell if a role is full-time or contractor based on the information extracted from Linkedin.

There could be other reasons that I'm missing here, but I wanted to clarify that the results and conclusions here might not be an accurate representation of the culture of a company. However, it is an interesting metric to check and I was curious to perform this analysis, therefore the motivation of this project.


## Linkedin API
Linkedin is one of the largest, if not the largest job boards. It takes the role of a professional social network where people add their portfolio and build their network. Therefore, it is the logical place to retrieve our data.

Linkeding doesn't have a free open API, but thanks to Open Source we can find unofficial APIs like the one we are using here: https://github.com/tomquirk/linkedin-api. This comes with some drawbacks though: 
- The API is not official, so if too many requests are done, the Linkedin account could be banned. I'm using it only as a personal project so it shouldn't be a risk, but I'm creating a temporary account for the purpose of this project
- I can only extract employees experience data up to 3rd degree connections in my network on Linkedin. I would need to connect with many people with my dummy account in order to access more contacts, which is not feasable. One solution could be to use the 7 day Premium trial on my dummy account, but I haven't tested that yet
- The API only returns the last 5 job experiences, but that might be enough for our purposes


## Open Source collaboration
When I first thought about working on this side project, I created my VMP in order to do a first test and see if it would work. However, I realised that the API endpoint I needed to use wasn't working. This is because Linkedin recently implemented some changes in their API endpoint that broke the existing logic.

Therefore, I decided that this was a good opportunity to contribute to an open source community and therefore I took a look at the issue. After playing around doing some reverse engineering with the endpoints that Linkedin calls (checking the "Networks" tab on the browser) and looking at discussions on a Github issue about the topic, I implemented my changed, tested it and created a PR: https://github.com/tomquirk/linkedin-api/pull/332

Having fixed the issue, I was able to start with my project.


## Architecture and Logic

[ DIAGRAM ]

The logic to collect the data is as follows:
1. Run `search_companies(keywords = company_to_look)` in order to find the company's URN (ID) I want to analyse
2. Search for past and present employees with 
    - `search_people(current_company=[searched_company_urn], limit=limit)`
    - `search_people(past_companies=[searched_company_urn], limit=limit)`
5. For each employee URN, run `api.get_profile(urn_id = urn)` which will retrieve all employee's experience and education

This data is stored in a raw `employees_experience` and `employees_education` buckets in Google Cloud Storage which is later transformed to perform different analysis.

These experience and education files are consumed by an external BigQuery table.

Then, DBT core is triggered to transform the data in the tables needed for analysis. Queries are stored in `dbt_project.models/` folder, structured by layers as best practices of [DBT suggest](https://docs.getdbt.com/guides/best-practices/how-we-structure/1-guide-overview).

I used a virtual environment to keep packages and versions isolated from other work.

Why did I use Google Cloud Platform for this project? GCP has a really generous free tier for my personal project and I am already familiar with it since I use it at my actual job. However, I'd like to get familiar with AWS too since it is widely used in many companies. I don't think it will be a big challenge since tools can be picked up quickly if one has the basics and best practices of Data Architecture covered.

Note:
This is the first draft of what was supposed to be the architecture of the project:

[ DIGRAM 2]

I planned to run the script on a Cloud Function triggered from my local every time I had a new target company to analyse. However, Linkedin API returns a [CHALLENGE error](https://github.com/tomquirk/linkedin-api#i-keep-getting-a-challenge) that is known in the package and I haven't had the chance and time to solve.


## Formula
I've attempted to score from 0 to 10 how good a company culture is based on the given employee data:
- Data roles have higher weight on the equation. Then followed by Software Engineering roles and rest of roles
- Comparison of time in target company to time in previous company is important
- Internships are excluded and contractors (can we find out this?)
- We take as benchmark an average of the top 5 and last 5 companies to work for and use that as MIN and MAX for our scoring
- Scale the data between 0 and 10
- Younger people have higher weight??

Variables for employees that have left:
- Number of employees that leave company every month. Get an average. The lower the better
- Time stayed in this company compared average time stayed in others.

Variables for employees currently in the company:
- How many have surpassed the time that they stay compared to previous companies compared to the total of employees


**Other applications**
This data might be useful for recruiters. Or to understand competitors?
