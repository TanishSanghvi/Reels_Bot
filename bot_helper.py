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
import nick_etl_helper
from random import choice


def date_calculation(today = datetime.date.today()):
    '''
    Used to get current day number and date of week ending
    
    Paramters:
        - today: Takes in the current date
    Returns:
        - date_of_week_ending_in: returns date of week ending 

    '''
    day_int = (today.weekday() + 1) % 7
    date_of_week_ending_in= (today - datetime.timedelta(1+day_int))
    
    return date_of_week_ending_in

def connect_to_database():
    '''
    Connects to the SQL database. Used multiple times throughout the code for different purposes such as 
    getting past reels data, push new updated data, get aws credentials and more
    
    Returns: 
        - psy_con: Takes in the connection URL and connects to the database
        - sqlalchemy_engine: Alternative method but does the same thing - takes in the connection URL and connects to the database
    '''
    
    import psycopg2
    from sqlalchemy import create_engine
    
    conn_string = '*Enter db credentials*'
    sqlalchemy_conn_string='*Enter connection string*'
    print('Connecting to the DB..')
    
    psy_con = psycopg2.connect(conn_string)
    sqlalchemy_engine = create_engine(sqlalchemy_conn_string)
    print('Done')
    
    return psy_con , sqlalchemy_engine

def requests_retry_session(retries=30,backoff_factor=0.3,
                           status_forcelist=(500, 502, 504),
                           session=None):
    '''
    Tries to request a reconnection to the in-progress session
    
    Parameters:
        - retries: number of retires
        - session : the sessions we are trying to reconnect to

    Returns:
        - session : Reconnected session
    '''
    
    import requests
    from requests.adapters import HTTPAdapter
    from requests.packages.urllib3.util.retry import Retry
    
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def connect_to_s3(bucket_name = 'vds'):
    '''
    Connects to S3 bucket using boto and by pulling AWS credentials
    
    Parameters:
        - bucket_name: the bucket where we want to store the file
    
    Returns:
        - bucket: returns a connection the bucket
    '''
    
    import boto
    #AWS Parameters
    aws_access_key_id, aws_secret_access_key = get_aws_credentials()
    print('Connecting to the S3..')
    s3_con = boto.connect_s3(aws_access_key_id, aws_secret_access_key)
    bucket = s3_con.get_bucket(bucket_name, validate=False)
    print('Done')
    
    return bucket

def get_aws_credentials():
    '''
    Returns stored aws credentials to push data to s3 bucket (from where it is pushed to the db)
    '''
    aws_access_key_id ='*AWS access key id*'
    aws_secret_access_key = '*AWS access key*'
    return aws_access_key_id,aws_secret_access_key

def ProgressPercentage(so_far, total):
    '''
    Used to check the percentage of bytes that have been transferred when pushing the file to S3

    '''
    import sys
    sys.stdout.write(
        str(so_far)+" bytes of "+str(total)+" bytes transferred! "+str(so_far*100/total)+"% Complete.\n")
    sys.stdout.flush()

def get_instagram_credentials(account_number): 
    '''
    Picks up a username and password from the list of accounts (stored in accounts_not_used.pkl)
    
    Parameters: 
        - account_number: default tries the first account from the list of accounts
    
    Returns: 
        - user_name: IG username
        - password: IG passowrd
    '''
    
    import random
    import dill
    path='accounts_not_used.pkl'
    try:
        accounts = dill.load(file = open(path,'rb'))
        if not len(accounts): raise KeyError
    except:
        accounts = {
            'account1':'password1',
            'account2':'password2',
            'account3':'password3'
            }
    account_number = random.choice(range(len(accounts)))
    user_name=list(accounts.keys())[account_number]
    password =list(accounts.values())[account_number]
    accounts.pop(user_name,0)
    dill.dump(accounts, file = open(path,'wb'))
    return user_name, password

def login_to_instagram(account_number=1, get_browser=False, headless=False):
    '''
    Uses Selenium along with certain Web driver configurations to log into Instagram. Additionally also get the cookies to avoid logging in repeatedly
    
    Parameters: 
        - account_number: Used when pulling credentials from get_instagram_credentials
        - get_browser: to check if browser needs to be returned
        - headless: if need the window to run in the background (minimized)
    
    Returns: 
        - cookies: List of cookie key values
    '''
    
    from selenium import webdriver
    from webdriver_manager.chrome import ChromeDriverManager
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from bs4 import BeautifulSoup as BS
    import time
    import dill
    path='accounts_reported.pkl' #Accounts have a tendency to be blocked by IG inrastructure. Blocked accounts are stored here and need to be unblocked manually
    try:
        accounts_reported = dill.load(file = open(path,'rb'))
    except:
        accounts_reported =list()

    if headless: #To allow the scraper to run in the background (minimized) as IG recognises bot otherwise
        options = webdriver.ChromeOptions()
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--headless")
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36")
        browser = webdriver.Chrome(ChromeDriverManager().install(),options = options)
    else:  
        browser = webdriver.Chrome(ChromeDriverManager().install())
    while True:
        browser.get("https://www.instagram.com")
        WebDriverWait(browser, 10).until(EC.title_is("Instagram"))
        username , password = bot_helper.get_instagram_credentials(account_number) #Get login credentials
        time.sleep(2)
        print('Logging into @{} on Instagram '.format(username)) #Using selenium to enter credentials
        try:
            browser.find_element_by_xpath('/html/body/div[1]/section/main/article/div[2]/div[1]/div/form/div[2]/div/label/input').send_keys(username)
        except:
            browser.find_element_by_xpath('/html/body/div[1]/section/main/article/div[2]/div[1]/div/form/div/div[1]/div/label/input').send_keys(username)
        try:
            browser.find_element_by_xpath('/html/body/div[1]/section/main/article/div[2]/div[1]/div/form/div[3]/div/label/input').send_keys(password)
        except:
            browser.find_element_by_xpath('/html/body/div[1]/section/main/article/div[2]/div[1]/div/form/div/div[2]/div/label/input').send_keys(password)
        try:
            browser.find_element_by_xpath('/html/body/div[1]/section/main/article/div[2]/div[1]/div/form/div[4]/button/div').click()
        except:
            browser.find_element_by_xpath('/html/body/div[1]/section/main/article/div[2]/div[1]/div/form/div/div[3]').click()
        time.sleep(2)
        try:
            browser.find_element_by_xpath('/html/body/div[4]/div/div/div/div[3]/button[2]').click()
        except:
            pass

        time.sleep(10)
        soup = BS(browser.page_source)
        
        try:
            cookies = browser.get_cookies()
            if get_browser: return browser
            browser.close()
            print('Authenticated!')
            cookies = [cookie['name']+'='+cookie['value'] for cookie in cookies]
            break
        except:
            pass
            
        if sum([1 for but in soup.find_all('button') if but.text.strip()!='Save Info' and but.text.strip()!='Not Now']): #If IG shows this text on logging in it indicates that the bot was recognised and it will be blocked
            if username in accounts_reported : continue    
            #Automailer to inform if an account has been blocked
            ta='*Enter your email address*'
            fromaddr = "*Enter automailer email address*"
            subject="Instagram bot blocked"
            body=f'{username} blocked.\n\n Please look look into this.'
            send_mail(to=ta,fromaddr=fromaddr,subject=subject,body=body)
            accounts_reported.append(username) 
            dill.dump(accounts_reported, file = open(path,'wb')) #Blocked accounts are reported in a file
            continue
        
        text=sum([1 for t in soup.find('div',{'id':'react-root'}).section.main.div.div.div.section.div.find_all('div') if t.text=='Save Your Login Info?']) #This text generally indicates that the bot was able to successfully log in
        if text:
            cookies = browser.get_cookies()
            if get_browser: return browser
            browser.close()
            print('Authenticated!')
            cookies = [cookie['name']+'='+cookie['value'] for cookie in cookies]
            break

    return cookies

def get_automailer_password():
    '''
    Connects to the database to pull the automailer password. Gets used in the send_mail function
    '''
    
    import pandas.io.sql as psql
    psy_con,_= connect_to_database()
    password_query="select * from mailer_bot"
    password_df = psql.read_sql(password_query, psy_con)
    return password_df.string[0]


def send_mail(to,fromaddr,cc='',bcc='',subject='',path_to_file=False,body='',pwd=False,body_is_html=False):
    '''
    Uses the MIME package to send mails from the automailer account to the concerned account. 
    Used to send alerts/weekly reels data with updated metric counts in tabular format using html functionality
    
    Parameters: 
        - to - Concerned user account
        - fromaddr - Automailer account
        - cc - If any users that need to be cc'ed
        - bcc - If any users that need to be bcc'ed'
        - subject - Subject of the mail
        - path_to_file - If need to add attachments
        - body - Body of the mail
        - pwd - password for automailer account
        - body_is_html - If content needs to be in HTML format

    '''
    
    print('Sending mail from account:{0}'.format(fromaddr))
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.mime.base import MIMEBase
    from email import encoders
    
    msg = MIMEMultipart() 
    msg['To'] = to
    msg['From'] = fromaddr
    msg['Cc'] = cc
    msg['Bcc'] = bcc
    msg['Subject'] = subject
    
    msg.add_header('Content-Type','text/html')
    if body_is_html: 
        body_type='html'
    else:
        body_type='plain'
    msg.attach(MIMEText(body,body_type))
    
    if path_to_file: # To send file attachments along with the mail
        attachment = open(path_to_file, "rb")
        part = MIMEBase('application', 'octet-stream')
        part.set_payload((attachment).read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', "attachment; filename= %s" % path_to_file.split('\\')[-1])
        msg.attach(part)
    
    if pwd:
        account_password=pwd
    else:
        account_password=get_automailer_password() # Get automailer login credentials if not passed as input
    
    domain_name=fromaddr.split('@')[1] #Different session objects for different mail domains
    if domain_name=='gmail.com': 
        server = smtplib.SMTP_SSL('smtp.gmail.com', 465) 
    else:
        server = smtplib.SMTP('smtp-mail.outlook.com', 587)
        server.starttls()   
        
    server.login(fromaddr, account_password)
    text = msg.as_string()
    server.sendmail(msg['From'],msg["To"].split(",") + msg["Cc"].split(",") + msg["Bcc"].split(","), text)
    server.quit()
    print ("Mail sent Successfully")

def ist_to_est(x):
    '''
    To change datetime from Indian Standard Time to Eastern Standard Time
    
    Parameteers:
        x: take the datetime as the input
    
    Returns:
        dateeastern: return the converted datetime
    '''
    
    import pytz
    format_used='%Y-%m-%d %H:%M:%S'
    date=dt.strptime(x,format_used)    
    gmt = pytz.timezone('asia/kolkata')
    eastern = pytz.timezone('US/Eastern')
    dategmt = gmt.localize(date)
    dateeastern = dategmt.astimezone(eastern)
    return dateeastern