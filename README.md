# Reels_Bot

Bot for daily scraping and subsequent update of reel-based data of various users from Instagram

## ABOUT

The goal of the project was to find relevant/trending topics from a large data set consisting of social media posts related to 'Nickelodeon' on Instagram. This was done using basic NLP followed by semantic-based models such as LDR and Gensim. A manual analysis of the data showed that LDR performed better in finding relevant topics as compared to Gensim.

## PROCESS SPECIFICATIONS
- Runs daily
- Manual effort: 5-10 mins
- Scrapes Instagram Reels data from a given list of handles
- Scrolls through the handle page (IG web pages are equppied with endless scrolling) to pull the reel ID and subsequent metrics such as video_views and video_plays
- The data is updated in a SQL database as well as sent on email using an automailer
- Some FYI's:
       - While all the reels from a handle are scraped, only reels published in the last 90 days are updated (saturation period of views)
       - The bot is equipped to run with a headless window for optimal runtime performance
       - Numerous IG bots are shuffled and used for scraping as bot accounts often get identified and blocked. If blocked, the accounts need to be unblocked manually

## STEPS

Step 1: To pull reel_ID's from IG handles 
- Our bots scrape the reels and push them to a temporary db table
- Runtime: Around 40 minutes, Manual Effort: N/A
- Code: reels_bot_part1.py

Step 2: To update metrics for reels published in the last 90 days
- All the manual effort and account blocking takes place in this step
- Accounts are shuffled and given different sleep rates to optimally updates as many reels as possible
- Runtime: Around 20 mins, Manual Effort: 10 mins
- Once updated, the data is pushed into a s3 bucket from where it is added to the dadatbase

## BUILT WITH / MODULES USED

Built with Python 3.7. Modules used:
 - BeautifulSoup
 - Selenium
 - Psycopg2 / sqlalchemy 
 - Boto3
 - Smptlib and Mime

## USE CASES
 - Reels are relatively new and data about it is someting that the IG API and other social media tools still lack to provide
 - With the rise in its use, this is a useful and in-demand tool among M&E companies who are in constant need to track their social media performances




