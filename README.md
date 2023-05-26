# Scraping odds for NBA, MLB, NHL

This python module can be use to compile odds from the major sports leagues from a public API. It can output the data to a dataframe for immediate analysis or send it to a MySQL database for storage via pymysql.

The API has a decent amount of historical data stored but I have not tested the limits of the historical cutoff dates. All the leagues atleast have data available going back 2020-2021 seasons, although some of the specific props are not as frequent. 

The scraping utilizes Selenium and takes a minutes to run through all the props/dates with the API. It has only been tested using a Firefox driver. The page source data is also store in the scraper object if different extraction is required. The page source includes team and player ids. 

A pipfile contains the required python packages.


# Examples

** loadDb note: <i>I use a table for player name and ID and a table for props that includes playerId the loadDb function is setup to accommodate this set up.</i>

### Initiating the class then option 1 or 2
```
import actNetScrape as ans

# assign class
odds = ans.actNetScraper()
```

### Option one - single league scrape, db save optional
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
df = odds.processScrapes(leauge = league, 
                        dates = dates
)

# if desired, save to existing database
odds.loadDb(df_props = df, 
            pymysql_conn_str = conn_details,
            update_players = True
)
```


### Option two - multiple league scrape AND save to database
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
    df = odds.processScrapes(league=league, 
                            dates=dates
    )
    df_odds.append(df)

    # store in database
    odds.loadDb(df_props = df, 
            pymysql_conn_str = conn_details,
            update_players = True
    )
```