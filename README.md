# Welcome to my ETL Project

This is a project I did that was inspired by my work. I wanted to make an ETL pipeline for analysis. The question being asked is "Is the cause of a drought influenced by rain
and or soil elements?". There are data sets that can get us this answer. Specifically in Mississippi we experienced a drought in summer of 2023 into winter of 2024.

Now that we have a question lets start digging in to the data.

## About the Data
### Drought Data
Our first dataset we will be using the U.S. Drought Monitor's Drought Severity and Coverage Index (DSCI). Link---> https://droughtmonitor.unl.edu/About/AbouttheData/DSCI.aspx

The data is broken down by counties by state. Our area of interest is Mississippi

Data will be extracted via api and the DSCI is a metric by the week (from Tuedsay-Monday)

### MS city data
Since the drought data's granularity goes to county and our weather datas granularity goes to city, we will extract city and county data. This is the link we will use to join drought data to the weather data. We will scrape the cities that belong to each county in Mississippi and only focus on the highest populated city in each county. Aata is located on wikipedia. we will have to scrape it. Link --> https://en.wikipedia.org/wiki/List_of_municipalities_in_Mississippi

### Weahter data
We will extract weather data from visual crossing via api. link--> https://www.visualcrossing.com/weather-api/

The output is a json. This contains precipitation and all soil data. Since the granularity goes down to city and by date. Since the DSCI is a metric that uses a week, we will aggregate the precipitation by sum and use week and average all our soil fields by the DSCI week.

## About this notebook
The folder prod_code is the production code for how I run the etl pipeline. I thought Jupyter Notebooks is a great way to give you my approach of extracting, transforming, and give you simple examples of the ETL pipeline.

## Final product
After the data is all extracted and transformed we will load it into a duckdb. From there we will extract the duckdb to a CSV and create a tableau dashboard
Link--> https://public.tableau.com/app/profile/jeremy.mccormick/viz/MSDroughtWeatherCorrelationTracker/Dashboard


Now lets get started
