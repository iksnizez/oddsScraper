import json, time, pymysql
import pandas as pd
import numpy as np
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup
from sqlalchemy import create_engine

class actNetScraper:
    
    def __init__(self):
        self.over_ids = {
            'nba': [42,34,40,30,36,38,341,346,345,344,343],
            'mlb':[506,498,508,56,54,58,52,60,100,500,62],
            'nhl':[50,64,564,568]
        }
        self.under_ids = {
            'nba':[43,35,41,31,37,39,342,350,349,348,347],
            'mlb':[507,499,509,57,53,59,55,61,101,501,63],
            'nhl':[51,65,565,569]
        }
        self.urls = {
            'nba':'https://api.{site}.com/web/v1/leagues/4/props/{proptype}?bookIds=369&date={date}',
            'mlb':'https://api.{site}.com/web/v1/leagues/8/props/{proptype}?bookIds=369&date={date}',
            'nhl':'https://api.{site}.com/web/v1/leagues/3/props/{proptype}?bookIds=369&date={date}'
        }
        self.map_option_ids = {
            'nba':{
                'core_bet_type_27_points':{'o':42, 'u':43, 'type':'pts', 'html_str_responses':[]},
                'core_bet_type_23_rebounds':{'o':34, 'u':35, 'type':'reb', 'html_str_responses':[]},
                'core_bet_type_26_assists':{'o':40, 'u':41, 'type':'ast', 'html_str_responses':[]},
                'core_bet_type_21_3fgm':{'o':30, 'u':31, 'type':'threes', 'html_str_responses':[]},
                'core_bet_type_24_steals':{'o':36, 'u':37, 'type':'stl', 'html_str_responses':[]},
                'core_bet_type_25_blocks':{'o':38, 'u':39, 'type':'blk', 'html_str_responses':[]},
                'core_bet_type_85_points_rebounds_assists':{'o':341, 'u':342, 'type':'pra', 'html_str_responses':[]},
                'core_bet_type_86_points_rebounds':{'o':346, 'u':350, 'type':'pr', 'html_str_responses':[]},
                'core_bet_type_87_points_assists':{'o':345, 'u':349, 'type':'pa', 'html_str_responses':[]},
                'core_bet_type_88_rebounds_assists':{'o':344, 'u':348, 'type':'ra', 'html_str_responses':[]},
                'core_bet_type_89_steals_blocks':{'o':343, 'u':347, 'type':'sb', 'html_str_responses':[]}
            },
            'mlb':{
                'core_bet_type_37_strikeouts':{'o':62, 'u':63, 'type':'k', 'html_str_responses':[]},
                'core_bet_type_74_earned_runs':{'o':500, 'u':501, 'type':'er', 'html_str_responses':[]},
                'core_bet_type_42_pitching_outs':{'o':100, 'u':101, 'type':'o', 'html_str_responses':[]},
                'core_bet_type_36_hits':{'o':60, 'u':61, 'type':'h', 'html_str_responses':[]},
                'core_bet_type_32_singles':{'o':52, 'u':53, 'type':'single', 'html_str_responses':[]},
                'core_bet_type_35_doubles':{'o':58, 'u':59, 'type':'dbl', 'html_str_responses':[]},
                'core_bet_type_33_hr':{'o':54, 'u':55, 'type':'hr', 'html_str_responses':[]},
                'core_bet_type_34_rbi':{'o':56, 'u':57, 'type':'rbi', 'html_str_responses':[]},
                'core_bet_type_78_runs_scored':{'o':508, 'u':509, 'type':'rs', 'html_str_responses':[]},
                'core_bet_type_73_stolen_bases':{'o':498, 'u':499, 'type':'stlb', 'html_str_responses':[]},
                'core_bet_type_77_total_bases':{'o':506, 'u':507, 'type':'tb', 'html_str_responses':[]}
            },
            'nhl':{
                'core_bet_type_31_shots_on_goal':{'o':50, 'u':51, 'type':'sog', 'html_str_responses':[]},
                'core_bet_type_38_goaltender_saves':{'o':64, 'u':65, 'type':'gs', 'html_str_responses':[]},
                'core_bet_type_280_points':{'o':564, 'u':565, 'type':'pts', 'html_str_responses':[]},
                'core_bet_type_279_assists':{'o':568, 'u':569, 'type':'ast', 'html_str_responses':[]},
                'core_bet_type_313_anytime_goal_scorer':{'o':None, 'u':None, 'type':'ats', 'html_str_responses':[]},
                'core_bet_type_311_to_score_2_or_more_goals':{'o':None, 'u':None, 'type':'gs2plus', 'html_str_responses':[]},
                'core_bet_type_312_to_score_3_or_more_goals':{'o':None, 'u':None, 'type':'gs3plus', 'html_str_responses':[]},
                'core_bet_type_48_first_goal_scorer':{'o':None, 'u':None, 'type':'gs1st', 'html_str_responses':[]},
                'core_bet_type_310_last_goal_scoer':{'o':None, 'u':None, 'type':'gsLast', 'html_str_responses':[]},
            }
        }
        self.columns_players = ['playerId', 'player', 'abbr']
        self.player_list = []
        self.players_avail = False


    # scrape website for dates and league
    def scrape(self, league, dates, selenium_browser_path, sleep_secs = 2):
        try:
            league = league.lower()
            failed = []

            # open selenium browser
            service = Service(selenium_browser_path)
            driver = webdriver.Firefox(service=service)

            # looping through each prop type on the site  
            for pt in self.map_option_ids[league].keys():
                # looping through each date for a single prop in a the season
                for d in dates:
                    # formattin date to add to the url search params
                    frmt_date = d.replace("-", "")

                    # path to webdriver for selenium
                    try:
                        site = self.urls[league].format(site='actionnetwork', proptype= pt, date= frmt_date)
                        driver.get(site)
                    except:
                        failed.append(d)

                    #getting the site page_source data and adding it to the dictionary for storage
                    response = driver.page_source
                    self.map_option_ids[league][pt]['html_str_responses'].append(response)

                    time.sleep(sleep_secs)
        except Exception as e:
            print(e)
            driver.close()

        driver.close()      

    # process html
    def processScrapes(self, league, dates):
        league = league.lower()
        missing_dates = []

        columns = [
            'propId','playerId','teamId', 'gameId', 'date', 
            'prop', 'line', 'oOdds', 'uOdds', 'projValue', 
            'oImpValue', 'oEdge', 'oQual', 'oGrade',
            'uImpValue', 'uEdge', 'uQual', 'uGrade'
        ]
        df_props = pd.DataFrame(columns=columns)

        for i in self.map_option_ids[league].keys():
            # name of prop
            prop = self.map_option_ids[league][i]['type']
            
            # this will hold the player prop data. playerId = key, values = list of data
            all_props_single_type = {}
            
            # loop through each game date for each prop
            for d in range(0,len(dates)):
                # game date
                date = dates[d]
                
                # converting selenium page_source to html
                parsed_html = BeautifulSoup(self.map_option_ids[league][i]['html_str_responses'][d],
                                            "html.parser")
                # select the data from the html
                try:
                    html_target_element = parsed_html.find("div", {"id": "json"}).text
                except:
                    missing_dates.append([prop, date, d])
                    continue
                # convert the json string to a dictioanry
                json_single_date = json.loads(html_target_element)

                #checking if there are multiple books odds provided or none
                if json_single_date.get("statusCode") is not None:
                        continue
                else:
                    books = json_single_date['markets'][0]['books']
                    book_count = len(books)
                
                # if multiple books, use 369
                book = 0
                if book_count > 1:
                    for b in range(0,len(books)):
                        if books[b]['book_id'] == 369:
                            book = b
                        else:    
                            continue

                # looping through all of the props for a single type and single day
                for j in json_single_date['markets'][0]['books'][book]['odds']:

                        #props_single_date = []
                        entry = [np.nan] * (len(columns) - 1)

                        # check for odds for the prop on the date
                        if j.get("statusCode") is not None:
                            continue
                        else:
                            playerId = j['player_id']

                            # nhl has duplicate propIds, creating new propId by adding playerId to it
                            if league == 'nhl':
                                propId = int(str(j['prop_id']) + str(playerId))
                            else:
                                propId = j['prop_id']

                            ou_check = j['option_type_id']

                            # if the player is not in this dict, it will be added. if it is in then
                            # only the odds that are not present will be added
                            if all_props_single_type.get(propId) is None:
                                
                                entry[0] = playerId
                                entry[1] = j['team_id']
                                entry[2] = j['game_id']
                                entry[3] = date
                                entry[4] = prop
                                entry[5] = j['value']  # line
                                
                                #the null value from json comes through strange into pandas, forcing nan
                                if pd.isnull(j['projected_value']):
                                    entry[8] = np.nan
                                else:
                                    entry[8] = j['projected_value']
                                                
                                #overs and props that don't have over/under designated
                                if ou_check in self.over_ids[league] or ou_check not in self.under_ids[league]:
                                    entry[6] = j['money'] # over odds
                                
                                    #the null value from json comes through strange into pandas, forcing nan
                                    if pd.isnull(j['bet_quality'])   :
                                        entry[11] = np.nan
                                    else:
                                        entry[11] = j['bet_quality']
                                        
                                    # data point only available in the more recent games
                                    if j.get('implied_value') is not None:
                                        entry[9] = j['implied_value']
                                        entry[10] = j['edge']
                                        entry[12] = j['grade']
                                        
                                #unders
                                else:
                                    entry[7] = j['money'] # under odds
                                
                                    #the null value from json comes through strange into pandas, forcing nan
                                    if pd.isnull(j['bet_quality'])   :
                                        entry[15] = np.nan
                                    else:
                                        entry[15] = j['bet_quality']

                                    # data point only available in the more recent games
                                    if j.get('implied_value') is not None:
                                        entry[13] = j['implied_value']
                                        entry[14] = j['edge']
                                        entry[16] = j['grade']
                                
                                # loading over and under data to the prop id
                                all_props_single_type[propId] = entry
                            
                            # adding the over or under to the existing propId key
                            else:
                                #overs
                                if ou_check in self.over_ids[league] or ou_check not in self.under_ids[league]:
                                    all_props_single_type[propId][6] = j['money'] # over odds
                                
                                    #the null value from json comes through strange into pandas, forcing nan
                                    if pd.isnull(j['bet_quality'])   :
                                        all_props_single_type[propId][11] = np.nan
                                    else:
                                        all_props_single_type[propId][11] = j['bet_quality']
                                        
                                    # data point only available in the more recent games
                                    if j.get('implied_value') is not None:
                                        all_props_single_type[propId][9] = j['implied_value']
                                        all_props_single_type[propId][10] = j['edge']
                                        all_props_single_type[propId][12] = j['grade']
                                        

                                #unders
                                else:
                                    all_props_single_type[propId][7] = j['money'] # under odds
                                
                                    #the null value from json comes through strange into pandas, forcing nan
                                    if pd.isnull(j['bet_quality'])   :
                                        all_props_single_type[propId][15] = np.nan
                                    else:
                                        all_props_single_type[propId][15] = j['bet_quality']

                                    # data point only available in the more recent games
                                    if j.get('implied_value') is not None:
                                        all_props_single_type[propId][13] = j['implied_value']
                                        all_props_single_type[propId][14] = j['edge']
                                        all_props_single_type[propId][16] = j['grade']              
                try:
                    # gather player names
                    players = json_single_date['markets'][0]['players']
                    for p in players:
                        player = [p['id'], p['full_name'], p['abbr']]
                        self.player_list.append(player)
                    self.players_avail = True
                except: 
                    continue

            # store the data from this loop                
            temp = pd.DataFrame(all_props_single_type.values(), 
            index=all_props_single_type.keys(), 
            columns=columns[1:]
            ).reset_index(names=['propId'])
                
            df_props = pd.concat([df_props, temp])
        
        if self.players_avail:
            # aggregating player to single df 
            df_players = pd.DataFrame(self.player_list, columns=self.columns_players)           
            df_players.drop_duplicates('playerId', inplace=True)
            
            # merge player names to odds
            df_props = df_props.merge(df_players, on= 'playerId')

        return df_props

    # store scraped data in db
    def loadDb(self, df_props, pymysql_conn_str):
        # make sure data is formatted
        df_props.loc[:,'date'] = pd.to_datetime(df_props['date'])
        df_props = df_props.astype({"uOdds":"Int64","oOdds":"Int64"})

        # connect to db
        sqlEngine = create_engine(pymysql_conn_str, pool_recycle=3600)
        dbConnection = sqlEngine.connect()

        # load to db
        df_props.to_sql('odds', dbConnection, if_exists='append', index=False)
        
        dbConnection.close()