# Scraping odds for NBA, MLB, NHL

This python module can be use to compile odds from the major sports leagues from an public API. It can output the data to a dataframe for immediate analysis or send it to a database for storage via pymysql.

The API has to have a decent amount of historical data stored but I have not tested the actual cutoff dates. All the leagues atleast have data available going back 2020-2021 seasons, although some of the specific props are not as frequent. 

The scraping utilizes Selenium and takes a minutes to hit the API. It has only been tested using a Firefox driver.

A pipfile contains the required python packages.


### Examples

Initiating the class then option 1 or 2
```
import actNetScrape as ans

# assign class
odds = ans.actNetScraper()
```

Option one - single league scrape
```
# league and date list
dates = ['2023-05-24', '2023-05-22', '2023-05-21', '2023-05-20']
league = 'nhl'

conn_details = <pymysql connection string>

# scrape the api
odds.scrape(league=league, 
            dates=dates, 
            selenium_browser_path= r"..\browser\geckodriver.exe", 
            sleep_secs = 2
)

# process page_source to JSON to df
df = odds.processScrapes(league, dates)

# if desired, save to database
# store in database
odds.loadDb(df, conn_details)
```

Option two - multiple league scrape AND save to database
```
# league and date list
leagues = ['mlb', 'nhl']
dates = ['2023-05-24', '2023-05-22', '2023-05-21', '2023-05-20']

# store dataframes
df_odds = []

# looping through the leagues and scraping the data
for league in leagues:
    conn_details = <pymysql connection string>

    # scrape the api
    odds.scrape(league=league, 
                dates=dates, 
                selenium_browser_path= r"..\browser\geckodriver.exe", 
                sleep_secs = 2
    )

    # process page_source to JSON to df
    df = odds.processScrapes(league=league, dates=dates)
    df_odds.append(df)

    # store in database
    odds.loadDb(df, conn_details)
```