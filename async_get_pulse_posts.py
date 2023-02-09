from selenium import webdriver
import time
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup as bs
import pandas as pd
import requests
import os
from lxml import html
import re
from IPython.display import clear_output
from tqdm import tqdm
import datetime
import locale
import asyncio
import aiohttp


# https://habr.com/ru/post/667630/
# https://www.youtube.com/watch?v=87A1Rq0CGtE


locale.setlocale(locale.LC_TIME, 'ru_RU.UTF-8')
today = datetime.datetime.today().strftime('%Y-%m-%d')
yesterday = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')



def str_to_dt(x) -> str:
    """Function for create date from parcing text"""
    x0 = x.split()[0]
    if x0 == 'Сегодня':
        return today
    elif x0 == 'Вчера':
        return yesterday
    else:
        date_string = ' '.join(x.split()[:3])
        date = datetime.datetime.strptime(date_string, "%d %B %Y").strftime('%Y-%m-%d')
        return date




async def get_info(ticker):
    async with aiohttp.ClientSession() as session:
        url = f'https://www.tinkoff.ru/invest/stocks/{ticker}/pulse/'
        # params = {'q': city, 'APPID': '2a4ff86f9aaa70041ec8e82db64abf56'}

        async with session.get(url=url) as response:

            source_data = await response.text()
            soup = bs(source_data, 'lxml')

            posts = soup.find_all('div', {'class': 'pulse-posts-by-ticker__foOEcD'})
            logins = soup.find_all('span', {'class': 'pulse-posts-by-ticker__a8gxOZ'})
            posts_dt = soup.find_all('div', {'class': 'pulse-posts-by-ticker__c8gxOZ'})
            posts = [post.text for post in posts]
            logins = [login.text for login in logins]
            posts_dt = [posts_dt.text for posts_dt in posts_dt]

            df_posts = pd.DataFrame()
            df_posts['login'] = logins
            df_posts['post'] = posts
            df_posts['dt'] = posts_dt
            df_posts['posts_time'] = df_posts.dt.apply(lambda x: x.split()[-1])
            df_posts['posts_dt'] = df_posts.dt.apply(lambda x: str_to_dt(x))
            df_posts['ticker'] = ticker #security
            del df_posts['dt']

            print(df_posts)

            df_posts.to_csv(f'data/async_{ticker}.csv', index=False)
            # return df_posts
            #



async def main(tickers_):
    tasks = []
    for ticker in tickers_:
        tasks.append(asyncio.create_task(get_info(ticker)))

    for task in tasks:
        await task

tickers = ['SBER']

asyncio.run(main(tickers))






    #
# def parcing(security, length):
#     s = Service(ChromeDriverManager().install())
#     driver = webdriver.Chrome(service=s)
#     driver.get(f'https://www.tinkoff.ru/invest/stocks/{security}/pulse/')
#     page_length = driver.execute_script("return document.body.scrollHeight")
#
#     while page_length < length:
#         driver.execute_script(f"window.scrollTo(0, {page_length - 1800});")
#         page_length = driver.execute_script("return document.body.scrollHeight")
#
#     source_data = driver.page_source
#     soup = bs(source_data, 'lxml')
#
#     return soup
#
#
# def get_lst_posts(length):
#     all_posts = pd.DataFrame()
#     securities = ['SBER', 'GAZP', 'YNDX', 'LKOH', 'AFLT', 'VTBR', 'POLY',
#                   'RUAL', 'MTSS', 'ROSN', 'USDRUB', 'CHMF', 'PLZL', 'MGNT', 'FIVE',
#                   'TSLA', 'ORCL', 'NVDA', 'INTC', 'IBM', 'GOOG', 'AMZN', 'AMD', 'AAPL', 'MSFT']








# def str_to_dt(x) -> str:
#     """Function for create date from parcing text"""
#     x0 = x.split()[0]
#     if x0 == 'Сегодня':
#         return today
#     elif x0 == 'Вчера':
#         return yesterday
#     else:
#         date_string = ' '.join(x.split()[:3])
#         date = datetime.datetime.strptime(date_string, "%d %B %Y").strftime('%Y-%m-%d')
#         return date
#
#
# def parcing(security, length):
#     s = Service(ChromeDriverManager().install())
#     driver = webdriver.Chrome(service=s)
#     driver.get(f'https://www.tinkoff.ru/invest/stocks/{security}/pulse/')
#     page_length = driver.execute_script("return document.body.scrollHeight")
#
#     while page_length < length:
#         driver.execute_script(f"window.scrollTo(0, {page_length - 1800});")
#         page_length = driver.execute_script("return document.body.scrollHeight")
#
#     source_data = driver.page_source
#     soup = bs(source_data, 'lxml')
#
#     return soup
#
#
# def prepare_data(security, length):
#     soup = parcing(security, length)
#     posts = soup.find_all('div', {'class': 'pulse-posts-by-ticker__foOEcD'})
#     logins = soup.find_all('span', {'class': 'pulse-posts-by-ticker__a8gxOZ'})
#     posts_dt = soup.find_all('div', {'class': 'pulse-posts-by-ticker__c8gxOZ'})
#     posts = [post.text for post in posts]
#     logins = [login.text for login in logins]
#     posts_dt = [posts_dt.text for posts_dt in posts_dt]
#
#     df_posts = pd.DataFrame()
#     df_posts['login'] = logins
#     df_posts['post'] = posts
#     df_posts['dt'] = posts_dt
#     df_posts['posts_time'] = df_posts.dt.apply(lambda x: x.split()[-1])
#     df_posts['posts_dt'] = df_posts.dt.apply(lambda x: str_to_dt(x))
#     df_posts['ticker'] = security
#     del df_posts['dt']
#
#     df_posts.to_csv(f'data/{security}.csv', index=False)
#     return df_posts
#
#
# def get_lst_posts(length):
#     all_posts = pd.DataFrame()
#     securities = ['SBER', 'GAZP', 'YNDX', 'LKOH', 'AFLT', 'VTBR', 'POLY',
#                   'RUAL', 'MTSS', 'ROSN', 'USDRUB', 'CHMF', 'PLZL', 'MGNT', 'FIVE',
#                   'TSLA', 'ORCL', 'NVDA', 'INTC', 'IBM', 'GOOG', 'AMZN', 'AMD', 'AAPL', 'MSFT']
#
#     for security in tqdm(securities):
#         print(security)
#         last_posts = prepare_data(security, length)
#         all_posts = pd.concat([all_posts, last_posts], ignore_index=True)
#         clear_output()
#
#     all_posts.to_csv(f'data/last_days.csv', index=False)



# example scroll parcing
# import aiohttp
# import asyncio
# import json
#
# from toolz import get_in, assoc, concat
#
# url = "http://localhost:9200"
# index = "stocks"
# doc_type = "stock"
#
# query = {
#     "query": {
#         "bool": {
#             "filter": [
#                 {"term": {
#                     "symbol.keyword": "EXPE"
#                 }},
#                 {"range": {
#                     "open": {
#                         "gt": 75.0,
#                         "lt": 100.0
#                     }
#                 }}
#             ]
#         }
#     }
# }
#
#
# async def async_scan(query, scroll_size):
#     # all_hits accumulates the result.
#     all_hits = []
#     async with aiohttp.ClientSession() as session:
#         # Create the primary scroll request.
#         scroll_id_request = assoc(
#             query, "size", scroll_size
#         )
#
#         async with session.post(
#                 "{}/{}/{}/_search".format(url, index, doc_type),
#                 params={"scroll": "1m"},
#                 json=scroll_id_request
#         ) as response:
#             read_response = await response.json()
#             scroll_id = read_response["_scroll_id"]
#             hits = get_in(["hits", "hits"], read_response, [])
#             all_hits.extend(hits)
#
#         # Loop until there are no more hits returned.
#         while hits:
#             async with session.post(
#                     "{}/_search/scroll".format(url),
#                     json={
#                         "scroll": "1m",
#                         "scroll_id": scroll_id
#                     }
#             ) as response:
#                 read_response = await response.json()
#                 hits = get_in(["hits", "hits"], read_response, [])
#                 all_hits.extend(hits)
#
#     return all_hits
#
#
# event_loop = asyncio.get_event_loop()
#
# tasks = asyncio.ensure_future(async_scan(query, 10))
#
# # Returns a list.
# results = event_loop.run_until_complete(tasks)
#
# print("Number of results: {}.".format(len(results)))
# print("First result: {}.".format(json.dumps(results[0])))
#
# event_loop.close()