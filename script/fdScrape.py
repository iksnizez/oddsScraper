import json, time, pymysql, re, random
import pandas as pd
import numpy as np
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from sqlalchemy import create_engine
from datetime import datetime, date


class fdScraper():
    def __init__(self):
        self.urls = {
            'wnba':'https://sportsbook.{site}.com/navigation/wnba',
            }
        
    # map to conver fd names to mine
    self.prop_name_map = {
            "Player Points":"pts",
            "Player Rebounds":"reb",
            "Player Assists":"ast"
        }


    def scrapeToDf(self, league, selenium_browser_path):
        
        # open browser and initial webpage
        service = Service(selenium_browser_path)
        driver = webdriver.Firefox(service=service)
        site = self.urls[league.lower()]
        driver.get(site.format('fanduel'))
        time.sleep(5)

        # GET THE GAME URLS FROM THE WNBA PAGE
        ele = driver.find_elements(By.CSS_SELECTOR, "ul")

        urls = []
        # urls might consistently be in UL indx 5  ele[5]
        for i in ele:
            ays = i.find_elements(By.CSS_SELECTOR, "a")
            for a in ays:
                url = a.get_attribute("href")
                if "basketball/usa---wnba" in url:
                    urls.append(url)
        urls = set(urls)

        today = date.today()#.strftime("%Y-%m-%d")

        #will hold a lists for all props on the scraped day
        all_props = []

        # LOOP THROUGH THE GAME URLS TO AGGREGATE THE PROP DATA
        for url in urls:
            # update driver to game specific url
            driver.get(url)
            time.sleep(1)
            # extract data from url text
            gameId = url.split("-")[-1]
            teams = url.split("/")[-1].split("@")
            away = teams[0].replace("-", " ")
            home = " ".join(teams[1].split("-")[0:-1]).strip()

            # 20 is arbitrary and depends on how many markets fd has for the games
            # i haven't seen more than 10 but if they add more props besides the 3 it could increase
            for i in range(20):
                
                try:
                    # CSS selector path for the buttons on the page
                    selector = 'nav.am > ul:nth-child(1) > li:nth-child({}) > a:nth-child(1) > div:nth-child(1)'
                    button = driver.find_element(By.CSS_SELECTOR, selector.format(i))
                    
                    # retrieve the html label for the button to check for the ones needing a click
                    buttonName = button.get_attribute("aria-label")
                    if "Points" in buttonName or "Rebounds" in buttonName or "Assists" in buttonName:
                        # if buttons of interest, click to update the driver page for the data
                        button.click()
                        
                        # working through the html
                        main = driver.find_elements(By.CSS_SELECTOR, "main")
                        ul = main[0].find_elements(By.CSS_SELECTOR, "ul")
                        # there are usually 10-12 ul elements on a page from what I have seen
                        for u in ul:
                            # only interested in the ULs that have over under data
                            # the data is easiest to access by grabbing all of the text for the UL the working with the whole string
                            txt = u.text
                            if "OVER" in txt or "UNDER" in txt:
                        
                                # split the UL text, convert fd prop name to db name, and remove header
                                split_txt = txt.split("\n")
                                prop_name = self.prop_name_map[split_txt[0]]
                                split_txt = split_txt[3:]
                        
                                #loop through the data in player specific chunks
                                ## each player has 5 entries - name, o line, o odds, u line, u odds
                                chunk_n = 5
                                for c in range(0, len(split_txt), chunk_n):
                                    props = [today, gameId,home,away, prop_name]
                                    
                                    #need to filter out the text from the lines so can't just append the chunk
                                    player_data = split_txt[c:c+chunk_n]
                                    player_data[1] = float(re.findall("\d*\.\d*", player_data[1])[0])
                                    player_data[2] = int(player_data[2])
                                    player_data[4] = int(player_data[4])
                                    del player_data[3]

                                    #concat player to agg list
                                    all_props.append(props + player_data)
                    
                except Exception as e:
                    #print(e)
                    continue
        driver.close()

        #convert list of list to dataframe
        columns = [
            'date', 'gameId', 'homeTeam', 'awayTeam', 'prop', 
            'player', 'line', 'oOdds', 'uOdds'
        ]

        df = pd.DataFrame(all_props, columns = columns)

        #generating random prop ID for PK in the db
        df['propId'] = df.apply(lambda row: str(row.gameId)+ 
                                    str(abs(int(((row.line // 1) + row.oOdds + row.uOdds) * random.random() * 10))),
                                axis = 1)
        df['gameType']= "r"
        df['league'] = "wnba"               

        return df
    

    def loadDb(self, df_props, pymysql_conn_str, dbAction='append'):
        
        dbAction = dbAction.lower()

        # make sure data is formatted
        #df_props.loc[:,'date'] = pd.to_datetime(df_props['date'])
        df_props = df_props.astype({"uOdds":"Int64","oOdds":"Int64"})

        # connect to db
        sqlEngine = create_engine(pymysql_conn_str, pool_recycle=3600)
        
        # handling loads as transaction - useful when doing mult. leagues and one on 1/2 loads.
        with sqlEngine.connect() as dbConnection:
            tran = dbConnection.begin()

            try:
                # load to db
                df_props.to_sql('odds', dbConnection, if_exists=dbAction, index=False)               
                tran.commit()
                dbConnection.close()
            
            except Exception as e:
                print(e)
                tran.rollback()
                dbConnection.close()