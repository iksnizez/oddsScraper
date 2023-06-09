# Scraping odds for NBA, MLB, NHL - *actnetscrape module*

This python module can be use to compile odds from the major sports leagues from a public API. It can output the data to a dataframe for immediate analysis or send it to a MySQL database for storage via pymysql.

The API has a decent amount of historical data stored but I have not tested the limits of the historical cutoff dates. All the leagues atleast have data available going back 2020-2021 seasons, although some of the specific props are not as frequent. 

The scraping utilizes Selenium and takes a few minutes to run through all the props/dates with the API when doing a small number of days. Full seasons or months will take a couple hours as to not overload the public facing server. It has only been tested using a Firefox driver. The page source data is also store in the scraper object if different extraction is required. The page source includes team and player ids. 

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

conn_details = <insert pymysql connection string>
browser_path = <insert browser file path>

# scrape the api
odds.scrape(league=league, 
            dates=dates, 
            selenium_browser_path= browser_path, 
            sleep_secs = 2
)

# process page_source to JSON to df
df = odds.processScrapes(leauge = league, 
                        dates = dates
)

# if desired, save to existing database
odds.loadDb(df_props=df, 
            pymysql_conn_str=conn_details, 
            oddsTableName=<INSERT ODDS TABLE NAME>, 
            dbAction='append',
            update_players=True, 
            playerTableName=<INSERT PLAYER TABLE NAME>
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
                selenium_browser_path= browser_path, 
                sleep_secs = 2
    )

    # process page_source to JSON to df
    df = odds.processScrapes(league=league, 
                            dates=dates
    )
    df_odds.append(df)

    # store in database
    odds.loadDb(df_props=df, 
            pymysql_conn_str=conn_details, 
            oddsTableName=<INSERT ODDS TABLE NAME>, 
            dbAction='append',
            update_players=True, 
            playerTableName=<INSERT PLAYER TABLE NAME>
    )
```

# SCRAPING CURRENT DATE WNBA ODDS *fdscrape module*

\>>>>>>>>>>>>>>>>> FD STOPPED ME FROM USING THIS AFTER ABOUT A MONTH  <<<<<<<<<<<<<<<<

This module will scrape odds for the WNBA that are currently posted on the website. There is no historical function and only the current odds can be retrieved.



# Example

```
import fdScrape as fds

# assign class
odds = fds.fdScraper()

# selenium browser path
browser_path = <insert browser file path>

# call method to scrape site into dataframe
df = odds.scrapeToDf('wnba', browser_path)

#### IF LOADING DB IS DESIRED #####
conn_details = <insert pymysql connection string>

# call method to load df to database
# append or replace can be used. 
odds.loadDb(df, conn_details, "append")
```