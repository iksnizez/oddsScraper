import json, time, pymysql, re, random
import pandas as pd
import numpy as np

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By

from sqlalchemy import create_engine
from datetime import datetime, date

#importing credentials from txt file
with open("../../../../Notes-General/config.txt", "r") as f:
    creds = f.read()
creds = json.loads(creds)

base = r"https://{site}.com/wnba/props"
site = "unabated"

service = Service(r"..\..\browser\geckodriver.exe")
driver = webdriver.Firefox(service=service)
waitTime = 10
driver.get(base.format(site=site))
time.sleep(2)

# hold all aggregated data
df_all = pd.DataFrame(columns = ["date",'rowIdx', 'arowIdx', 'rowId', 'compId', 'player', 'homeTeam',
       'awayTeam', 'pos', 'line', 'oOdds', 'uOdds', 'prop'])

prop_names = {'pts':'PointsSIM', 
              'reb':'ReboundsSIM',
              'ast':'AssistsSIM',
              'threes':'Three Pointers Made'}

for k, v in prop_names.items():
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
                time.sleep(4)

                #### grab data #####
                # entire table 
                ele = driver.find_elements(By.CSS_SELECTOR,".ag-body-viewport")

                p_columns = ["date","rowIdx", "arowIdx", "rowId", "compId", "player", "homeTeam", "awayTeam", "pos"]
                player_agg = []
                
                players = ele[0].find_elements(By.CSS_SELECTOR,".ag-pinned-left-cols-container")
                for i in players[0].find_elements(By.CSS_SELECTOR,".ag-row"):
                    row_idx = i.get_attribute("row-index")
                    arow_idx = i.get_attribute("aria-rowindex")
                    row_id = i.get_attribute("row-id")
                    comp_id = i.get_attribute("comp-id")
                    
                    text_split = i.text.strip().split("\n")
                    date = text_split[0]
                    player = text_split[2].split("(")[0]
                    pos = text_split[2].split(")")[1].replace("","")
                    
                    locale = text_split[5]
                    if locale == "@":
                        home = text_split[5]
                        away = text_split[3]
                    else:
                        home = text_split[3]
                        away = text_split[5] 
                    
                    temp = [date, row_idx, arow_idx, row_id, comp_id, player, home, away, pos]
                    player_agg.append(temp)
                
                df_players = pd.DataFrame(player_agg, columns = p_columns)

                # odds
                o_columns =  ["rowIdx", "arowIdx", "rowId", "compId", "line", "oOdds", "uOdds", "prop"]
                odds = ele[0].find_elements(By.CSS_SELECTOR,".ag-center-cols-container")
                row_odds = []
                for i in odds[0].find_elements(By.CSS_SELECTOR,".ag-row"):
                    #print("row start >>")
                    row_idx = i.get_attribute("row-index")
                    arow_idx = i.get_attribute("aria-rowindex")
                    row_id = i.get_attribute("row-id")
                    comp_id = i.get_attribute("comp-id")
                    
                    # getting all the lines/odds for a player(row)
                    temp_row_odds = []
                    for j in i.find_elements(By.CSS_SELECTOR,".ag-cell"):
                        #skip books that don't have odds; they post blank cells as 2 spaces (len = 2)
                        if len(j.text) < 8:
                            continue
                        else:
                            txt_split_first = j.text.strip().split("\n") #['o10.5 -130', 'u10.5 +100']
                            
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
                
                                temp_row_odds.append([line, oOdds, uOdds])
                    
                    #finding most frequent line 
                    for j in temp_row_odds:
                        lines = {}
                       
                        l = j[0]
                        lines[l] = lines.get(l, 0) + 1
                            
                        best_over = -500000
                        best_under = -500000
                    
                    # finding best odds at the mode line to save
                    for j in temp_row_odds:
                        freq_line = max(lines, key=lines.get)
                        if j[0] == freq_line:
                            o = int(j[1])
                            u = int(j[2])
                    
                            if o > best_over: best_over = o
                            if u > best_under: best_under = u
          
                    temp = [row_idx, arow_idx, row_id, comp_id, freq_line, best_over, best_under, k]
                    row_odds.append(temp)
                    
                df_odds = pd.DataFrame(row_odds, columns= o_columns)
                
                # merge player info with odds
                df_players= df_players.merge(df_odds.drop(["rowIdx", "arowIdx", "compId"], axis=1), on='rowId')

                # add merged prop data to parent df 
                df_all = pd.concat([df_all, df_players])
                ####################
                break
            except:
                break
driver.close()


# filter dates if needed
dfF = df_all.copy()
#dfF = dfF[dfF['date']=='7/01']

dfF.loc[:,'propId'] = dfF.apply(lambda row: row['rowId'].replace("-", "") + row['prop'], axis=1)
dfF.loc[:,'gameId'] = dfF.apply(lambda row: row['rowId'].split("-")[1], axis=1)
dfF.loc[:,'playerId'] = dfF.apply(lambda row: row['rowId'].split("-")[0], axis=1)

dfF=dfF[['date', 'propId', 'playerId', 'gameId', 'player', 'homeTeam',
       'awayTeam', 'prop', 'line', 'oOdds', 'uOdds']]
dfF.columns = ['date', 'propId', 'playerId', 'gameId', 'player', 'homeTeam', 'awayTeam', 'prop','line','oOdds','uOdds']
dfF['gameType'] = "r"
dfF['league'] = "wnba"
#dfF.loc[:,'date'] = '2023-07-01'
dfF.loc[:,'date'] = datetime.today().strftime('%Y-%m-%d')

pymysql_conn_str = creds['pymysql']['wnba']
sqlEngine = create_engine(pymysql_conn_str, pool_recycle=3600)
conn = sqlEngine.connect()
dfF.to_sql('odds', conn, if_exists='append', index=False)