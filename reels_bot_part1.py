import pandas as pd
import requests
import re
import time
import warnings
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup as BS
from datetime import datetime as dt
from datetime import timedelta as td
from tqdm import tqdm
from multiprocessing.dummy import Pool as ThreadPool
from random import choice
import os
import bot_helper
warnings.filterwarnings('ignore')

working_directory = "/Users/apple/Desktop/Reels"
os.chdir(working_directory)

code_start_time = dt.now()
lag = 0

today = (dt.now()-td(lag)).date()
print('Running for : {}'.format(today))

#data extraction for the week in question
date_of_week_ending_in = bot_helper.date_calculation()+td(7)

#Creating our weekly directory
date_directory = working_directory+'/{0}'.format(date_of_week_ending_in.strftime('%Y-%m-%d'))
bot_helper.make_directory(date_directory)

def get_reels_data(page):
    videos= list()
    browser.get(f"https://www.instagram.com/{page}/reels")
    time.sleep(10)
    html=browser.page_source
    soup=BS(html,'html.parser')
    links=soup.find_all('a',href=True)
    pause_time = 0.5
    last_height = browser.execute_script("return document.body.scrollHeight")
    browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    
    if not sum([1 for link in links if link.get('href').startswith(f'/{page}/reels/')]):
        return videos
    while True:
        html=browser.page_source
        soup=BS(html,'html.parser')
        links=soup.find_all('a',href=True)
        videos.extend( [
            {
                'shortcode_id':link.get('href').replace('reel','').replace('/',''),
                'user_name':page
            } 
                 for link in links if link.get('href').startswith('/reel/')])
        time.sleep(pause_time)
        new_height = browser.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break;
        last_height = new_height
        browser.execute_script(f"window.scrollTo({last_height}, document.body.scrollHeight);")
    
    reels_df = pd.DataFrame(videos)
    reels_df = reels_df.drop_duplicates('shortcode_id').reset_index(drop=True)

    return reels_df

browser = bot_helper.login_to_instagram(get_browser=True, headless=True)

time.sleep(5)    

psy_con, engine = bot_helper.connect_to_database()

query = 'Select * from handles order by 3'
handles = pd.read_sql(query,engine)
pages = handles['user_username'].to_list()

no_reels = []
reels_to_update = pd.DataFrame()

for page in pages:
    temp_df = get_reels_data(page)
    try:
        reels_to_update = reels_to_update.append(temp_df, sort=False)
    except:
        no_reels.append(page)
    print (f"Page :{page}, Data: {len(temp_df)}, Total :{len(reels_to_update)}")
    time.sleep(2)
browser.close()

temp=reels_to_update.copy()

query=f"Select * from reels_bot where date_measured=(Select max(date_measured) from reels_bot) and is_deleted=0"
prev_day_df=pd.read_sql(query,engine)

reels_to_update=reels_to_update.merge(prev_day_df,on=['shortcode_id'],how='outer')
reels_to_update['user_name_x'].fillna(0,inplace=True)
reels_to_update['user_name']=reels_to_update.apply(lambda x: x['user_name_y'] if not x['user_name_x'] else x['user_name_x'], axis=1)
reels_to_update = reels_to_update.drop_duplicates('shortcode_id').reset_index(drop=True)

cookies = bot_helper.login_to_instagram(1)  

headers = {
    'authority': 'www.instagram.com',
    'cache-control': 'max-age=0',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'sec-fetch-site': 'none',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-user': '?1',
    'sec-fetch-dest': 'document',
    'accept-language': 'en-US,en;q=0.9,mr-IN;q=0.8,mr;q=0.7',
    'cookie': "; ".join(cookies)
}


imp_shows=['allthat','avatarthelastairbender','henrydangerforce','invaderzim','nickanimation',
'officialheyarnold','officialpatrickstar','spongebob','thecasagrandes','theloudhousecartoon']

reels_to_update['flag']=0
for i in range(0,len(reels_to_update)):
    if reels_to_update['user_name'][i] in imp_shows:
        reels_to_update['flag'][i]=1

reels_to_update = reels_to_update.sort_values(['flag'], ascending=[False]).reset_index()
reels_to_update.drop(['flag','index'], axis=1, inplace=True)

reels_to_update.to_csv(date_directory+"/"+'reels_backup.csv', index = False)