
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
warnings.filterwarnings('ignore')
import bot_helper

working_directory = "/Users/apple/Desktop/Reels"
os.chdir(working_directory)

code_start_time=dt.now()
lag=1

today=(dt.now()-td(lag)).date()
print('Running for : {}'.format(today))

date_of_week_ending_in=bot_helper.date_calculation()+td(7)

date_directory=working_directory+'/{0}'.format(date_of_week_ending_in.strftime('%Y-%m-%d'))
date_measured_directory=date_directory+'/{0}'.format(today)
bot_helper.make_directory(date_directory)

browser = bot_helper.login_to_instagram(get_browser=True, headless=True)

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


reels_to_update=pd.read_csv('reels_backup.csv')

s = bot_helper.requests_retry_session()

def get_video_details(shortcodeid):
    counter=0
    while True:
        try:
            response=s.get('https://www.instagram.com/reel/{0}/'.format(shortcodeid),
                        params={'__a':1},
                        headers=headers)
            return response
        except Exception as e:
            time.sleep(10)

problem_fetching_data=list()

def update_data(i):
    si=reels_to_update.loc[i,'shortcode_id']
    counter=0
    while True:
        counter=+1
        response = get_video_details(si)
        reels_to_update.loc[i,'status_code']= response.status_code
        try:
            details=response.json()
            shortcode_media = details['graphql']['shortcode_media']
            break
        except KeyError as ke:
            if counter>3: return None
            cookies = login_to_instagram(1)
            headers['cookie'] = "; ".join(cookies)
        except Exception as e:
            print(e)
            reels_to_update.loc[i,'status_code']= 0
            return None
    
    reels_to_update.loc[i,'video_duration_seconds']= shortcode_media['video_duration']
    reels_to_update.loc[i,'video_views']= shortcode_media['video_view_count']
    reels_to_update.loc[i,'video_plays']= shortcode_media['video_play_count']
    reels_to_update.loc[i,'post_comments_count'] = shortcode_media['edge_media_preview_comment']['count']
    reels_to_update.loc[i,'post_likes_count']= shortcode_media['edge_media_preview_like']['count']
    reels_to_update.loc[i,'post_title_text']= shortcode_media['edge_media_to_caption']['edges'][0]['node']['text']
    reels_to_update.loc[i,'date_published'] = ist_to_est(dt.fromtimestamp(shortcode_media['taken_at_timestamp']).strftime('%Y-%m-%d %H:%M:%S')).date()
    reels_to_update.loc[i,'user_name']= shortcode_media['owner']['username']
    reels_to_update.loc[i,'user_full_name']= shortcode_media['owner']['full_name']
    reels_to_update.loc[i,'page_id']= shortcode_media['owner']['id']
    
pc1=0
a=0
b=len(reels_to_update)

for i in range(a,b):
    try:
        update_data(i)
    except Exception as e:
        print(e)
    pc=round((i/(len(reels_to_update)))*100)
    if pc>pc1: print(f"{pc} % complete") ; pc1=pc
    
temp=reels_to_update.copy()

reels_to_update.reset_index(drop=True,inplace=True)
reels_to_update['post_title_text']=reels_to_update['post_title_text'].apply(lambda x: re.sub("[^a-zA-Z0-9,@#//:_ -]","",str(x)))

reels_to_update['video_duration_seconds'].fillna(0,inplace=True)
for col in ['post_likes_count','post_comments_count','video_views','video_plays']:
    reels_to_update[col].fillna(0,inplace=True)
    reels_to_update[col]=reels_to_update[col].astype(int)

reels_to_update['date_measured']=today
reels_to_update['is_deleted']=0

order=['page_id','user_full_name','user_name','post_title_text','date_published','date_measured','shortcode_id','video_duration_seconds','video_views','video_plays','post_comments_count','post_likes_count','is_deleted']
reels_to_update=reels_to_update[order]

reels_to_update.to_csv(date_measured_directory+"/"+'reels.csv', index = False)
reels_to_update.to_csv('reels_bot.txt',sep="^", index=False, encoding = 'utf-8')

full_key_name = 'vds_db/reels_bot.txt'

print('Data Prepared! Uploading to S3 Bucket...')

bucket = bot_helper.connect_to_s3()
k = bucket.new_key(full_key_name)
k.set_contents_from_filename('reels_bot.txt', replace=True, cb=ProgressPercentage)
k.set_metadata('Content-Type', 'text/csv')

print('Connected! Pushing data to the DB..')
psy_con, engine = bot_helper.connect_to_database()

sql_drop = """DROP TABLE IF EXISTS reels_bot_temp; """

sql_create="""
CREATE TABLE reels_bot_temp
(
  page_id                  FLOAT(1),
  user_full_name           VARCHAR(MAX),
  user_name            VARCHAR(MAX),
  post_title_text          VARCHAR(MAX),
  date_published           DATE,
  date_measured            DATE,
  shortcode_id             VARCHAR(MAX),
  video_duration_seconds   FLOAT(1),
  video_views              INT,
  video_plays              INT,
  post_comments_count      INT,
  post_likes_count         INT,
  is_deleted               bool
);
"""
aws_access_key_id,aws_secret_access_key = bot_helper.get_aws_credentials()

sql_copy = """
COPY reels_bot_temp
FROM 's3://vds/{2}' 
CREDENTIALS 'aws_access_key_id= {0};aws_secret_access_key={1}' 
REGION 'us-east-1' 
IGNOREHEADER 1 
DELIMITER '^';
""".format(aws_access_key_id,aws_secret_access_key,full_key_name)

sql_insert = f"""
Delete from reels_bot where date_measured ='{today}';
INSERT INTO reels_bot
SELECT page_id,
       user_full_name,
       user_name,
       post_title_text,
       date_published,
       date_measured,
       shortcode_id,
       video_duration_seconds,
       video_views,
       video_plays,
       post_comments_count,
       post_likes_count,
       is_deleted
FROM reels_bot_temp;"""


cursor = psy_con.cursor()
cursor.execute(sql_drop)
cursor.execute(sql_create)
cursor.execute(sql_copy)
cursor.execute(sql_insert)

psy_con.commit()
print('Execution Complete!')
print(psy_con.notices)


fromaddr = "mailerbot@gmail.com"
toaddr=['tanish.sanghvi@gmail.com']
subject='Reelsbot Status'
body='Reelsbot ran successfully <br><br>'


body+="".join(psy_con.notices)

query='''
Select date_measured,count(*) from reels_bot 
group by 1
order by 1 desc
limit 10;
'''

df = pd.read_sql_query(query,psy_con)
df['date_measured']=df['date_measured'].apply(lambda x : x.strftime("%d-%b-%Y"))

body+='<br><br><br>'
body+=df.to_html()
body+='<br>'


for ta in toaddr: 
    bot_helper.send_mail(ta,fromaddr,subject=subject,body=body,body_is_html=True)





    