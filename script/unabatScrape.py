import json, time, pymysql, re, random
import pandas as pd
import numpy as np

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By

from sqlalchemy import create_engine
from datetime import datetime, date

###################
###################
class unabatScraper:

    def __init__(self):
        self.urls = {
            'wnba':"https://{site}.com/wnba/props"
        }

        self.map_prop_names = {
            'pts':'PointsSIM', 
            'reb':'ReboundsSIM',
            'ast':'AssistsSIM',
            'threes':'Three Pointers Made'
        }

        self.all_columns = [
            "date",'rowIdx', 'arowIdx', 'rowId', 'compId', 'player', 'homeTeam',
            'awayTeam', 'pos', 'line', 'oOdds', 'uOdds', 'prop'
        ]

        self.player_columns = [
            "date","rowIdx", "arowIdx", "rowId", "compId", "player", "homeTeam", "awayTeam", "pos"
        ]

        self.odds_columns = [
            "rowIdx", "arowIdx", "rowId", "compId", "line", "oOdds", "uOdds", "prop"
        ]


    def scrape(self, league, selenium_browser_path, sleep_secs = 3):
        # url site
        site = "unabated"

        #open browser and initial site
        service = Service(selenium_browser_path)
        driver = webdriver.Firefox(service=service)
        driver.get(self.urls[league].format(site=site))
        time.sleep(sleep_secs)

        # will hold all aggregated data after it is joined
        df_all = pd.DataFrame(columns = self.all_columns)

        # looping through all the props of interest
        # this will select the buttons from the home page and click through to load the data
        for k, v in self.map_prop_names.items():
            # locate menu button and click it to expand menu options
            btn = driver.find_elements(By.CSS_SELECTOR, 'div.dropdown:nth-child(2) > button:nth-child(1)')
            btn[0].click()
            
            # locate the expanded menu that was hidden=False after the above click
            menu = btn[0].find_elements(By.XPATH, "/html/body/div[1]/div/div/div/div/div/div[1]/div[2]/div")

            #find all buttons in the expanded menu
            menuBtn = menu[0].find_elements(By.CLASS_NAME, "dropdown-item")
            # find the desired prop buttons, click them to load the page and grab data
            for p in menuBtn:
                if p.text == v:
                    try:
                        # click the button to load the new prop data
                        p.click()
                        time.sleep(sleep_secs)

                        
                        # entire table - left (contains player data), center (contains odds data)
                        ele = driver.find_elements(By.CSS_SELECTOR,".ag-body-viewport")

                        #### grab data player #####
                        # will aggregate the player info from each row into a list of list
                        player_agg = []
                        
                        players = ele[0].find_elements(By.CSS_SELECTOR,".ag-pinned-left-cols-container")
                        # loop through each row of the html to build player data
                        for i in players[0].find_elements(By.CSS_SELECTOR,".ag-row"):
                            #unabated identifier info - will be used to tie the player data to the odds data
                            row_idx = i.get_attribute("row-index")
                            arow_idx = i.get_attribute("aria-rowindex")
                            row_id = i.get_attribute("row-id")
                            comp_id = i.get_attribute("comp-id")
                            
                            # extracting the player info from the html row
                            text_split = i.text.strip().split("\n") # entire data text
                            date = datetime.today().strftime('%Y-%m-%d') 
                            player = text_split[2].split("(")[0]
                            pos = text_split[2].split("(")[1].replace(")","")
                            
                            # team data
                            locale = text_split[5]
                            if locale == "@":
                                home = text_split[5]
                                away = text_split[3]
                            else:
                                home = text_split[3]
                                away = text_split[5] 
                            
                            # aggregate the row data
                            temp = [date, row_idx, arow_idx, row_id, comp_id, player, home, away, pos]
                            #add the row to the parent list of all rows
                            player_agg.append(temp)

                        # convert all the rows to a dataframe
                        df_players = pd.DataFrame(player_agg, columns = self.player_columns)

                        #### grab data odds #####
                        odds = ele[0].find_elements(By.CSS_SELECTOR,".ag-center-cols-container")

                        # this is the parent list of list, it will hold individual row list 
                        row_odds = []
                        # looping through each row of odds data
                        for i in odds[0].find_elements(By.CSS_SELECTOR,".ag-row"):
                            #unabated row identifiers
                            row_idx = i.get_attribute("row-index")
                            arow_idx = i.get_attribute("aria-rowindex")
                            row_id = i.get_attribute("row-id")
                            comp_id = i.get_attribute("comp-id")
                            
                            # getting all the lines/odds for a player(row)
                            # temp_row_odds will hold all the diff. books odds for the players
                            temp_row_odds = []
                            for j in i.find_elements(By.CSS_SELECTOR,".ag-cell"):
                                #skip books that don't have odds; they post blank cells as 2 spaces (len = 2)
                                if len(j.text) < 6:
                                    continue
                                else:
                                    # split the rows data into an over and an under item
                                    txt_split_first = j.text.strip().split("\n") #['o10.5 -130', 'u10.5 +100']
                                    
                                    # loop over the first split to handle the over and under data extraction
                                    for h in txt_split_first:

                                        txt_split_second = h.split(" ")
                        
                                        # try: handles missing odds
                                        try:
                                            if "o" in txt_split_second[0]:
                                                oOdds = txt_split_second[1].replace("+", "")
                                                line = txt_split_second[0].replace("o", "")
                                            else:
                                                uOdds = txt_split_second[1].replace("+", "")
                                                
                        
                                        except Exception as e:
                                            print(e)
                                            oOdds = np.nan
                                            uOdds = np.nan
                                            line = np.nan

                                        # combine the over and under odds with the line
                                        temp_row_odds.append([line, oOdds, uOdds])
                            
                            ### finding most frequent line out of all the books
                            # temp_row_odds is a list of list - every list is the player, 
                            # the interior list are all the different odds
                            # this loop goes through each player
                            lines = {}
                            for j in temp_row_odds:
                                #lines = {}
                            
                                l = j[0]
                                lines[l] = lines.get(l, 0) + 1

                            # these will be overwritten with the best odds        
                            best_over = -500000
                            best_under = -500000
                            # finding best odds for the mode line to save
                            for j in temp_row_odds:
                                # retrieving the line with the highest count from the previous loop
                                freq_line = max(lines, key=lines.get)
                                if j[0] == freq_line:
                                    # determining if these odds are the best
                                    o = int(j[1])
                                    u = int(j[2])
                            
                                    if o > best_over: best_over = o
                                    if u > best_under: best_under = u

                            # aggregate the most freq. line and the best odds with the identifying data for the single player
                            temp = [row_idx, arow_idx, row_id, comp_id, freq_line, best_over, best_under, k]
                            # add this row data to the parent list of all players
                            row_odds.append(temp)

                        # convert parent list of list to dataframe for merging with players info dataframe   
                        df_odds = pd.DataFrame(row_odds, columns= self.odds_columns)
                        
                        # merge player info with odds
                        df_players= df_players.merge(df_odds.drop(["rowIdx", "arowIdx", "compId"], axis=1), on='rowId')

                        # add merged prop data to parent df 
                        df_all = pd.concat([df_all, df_players])

                        # once all the data for the k (prop) has been aggregated, the loop can be broken to start the next row
                        break
                    except:
                        break
        driver.close()

        # data frame clean up
        df_all.loc[:,'propId'] = df_all.apply(lambda row: row['rowId'].replace("-", "") + row['prop'], axis=1)
        df_all.loc[:,'gameId'] = df_all.apply(lambda row: row['rowId'].split("-")[1], axis=1)
        df_all.loc[:,'playerId'] = df_all.apply(lambda row: row['rowId'].split("-")[0], axis=1)

        df_all=df_all[['date', 'propId', 'playerId', 'gameId', 'player', 'homeTeam',
                        'awayTeam', 'prop', 'line', 'oOdds', 'uOdds']]
        df_all.columns = [
            'date', 'propId', 'playerId', 'gameId', 'player', 'homeTeam', 'awayTeam', 'prop','line','oOdds','uOdds'
        ]
        df_all['gameType'] = "r"
        df_all['league'] = "wnba"
        df_all.loc[:,'date'] = datetime.today().strftime('%Y-%m-%d')

        return df_all

def loadDb(self, df_props, pymysql_conn_str, oddsTableName='odds', dbAction='append'):
        sqlEngine = create_engine(pymysql_conn_str, pool_recycle=3600)

        with sqlEngine.connect() as dbConnection:
            tran = dbConnection.begin()

            try:
                # load to db
                df_props.to_sql(oddsTableName, dbConnection, if_exists=dbAction, index=False)            
                tran.commit()
                dbConnection.close()
            
            except Exception as e:
                print(e)
                tran.rollback()
                dbConnection.close()