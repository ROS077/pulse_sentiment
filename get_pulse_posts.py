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


def parcing(security, length):
    s = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=s)
    driver.get(f'https://www.tinkoff.ru/invest/stocks/{security}/pulse/')
    page_length = driver.execute_script("return document.body.scrollHeight")

    while page_length < length:
        driver.execute_script(f"window.scrollTo(0, {page_length - 1800});")
        page_length = driver.execute_script("return document.body.scrollHeight")

    source_data = driver.page_source
    soup = bs(source_data, 'lxml')

    return soup


def prepare_data(security, length):
    soup = parcing(security, length)
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
    df_posts['ticker'] = security
    del df_posts['dt']

    df_posts.to_csv(f'data/{security}.csv', index=False)
    return df_posts


def get_lst_posts(length):
    all_posts = pd.DataFrame()
    securities = ['SBER', 'GAZP', 'YNDX', 'LKOH', 'AFLT', 'VTBR', 'POLY',
                  'RUAL', 'MTSS', 'ROSN', 'USDRUB', 'CHMF', 'PLZL', 'MGNT', 'FIVE',
                  'TSLA', 'ORCL', 'NVDA', 'INTC', 'IBM', 'GOOG', 'AMZN', 'AMD', 'AAPL', 'MSFT']

    for security in tqdm(securities):
        print(security)
        last_posts = prepare_data(security, length)
        all_posts = pd.concat([all_posts, last_posts], ignore_index=True)
        clear_output()

    all_posts.to_csv(f'data/last_days.csv', index=False)


def posts_update():
    get_lst_posts(length=500000)
    main_df = pd.read_csv('data/main.csv')
    last_posts_df = pd.read_csv('data/last_days.csv')

    # крайняя дата всех постов
    max_dt = main_df.posts_dt.max()
    print(f'Последняя дата в основной сборке постов - {max_dt}')
    # минимальная дата постов для тикера с наименьшим охватом дат
    min_df_last_posts = last_posts_df.groupby('ticker').posts_dt.min().values.max()
    print(f'Минимальная дата в последней сборке постов - {min_df_last_posts}')
    # максимальная дата постов из последней сборки
    max_df_last_posts = last_posts_df.posts_dt.max()
    lst_posts = last_posts_df[
        (last_posts_df['posts_dt'] > max_dt) & (last_posts_df['posts_dt'] < max_df_last_posts)]
    # minimal dt of lst_posts
    min_dt_last = lst_posts.posts_dt.min()

    if max_dt >= min_dt_last:
        main = main_df[main_df.posts_dt < min_dt_last]
        main = pd.concat([main, lst_posts], ignore_index=True)
        main.to_csv(f'data/backup/main_{today.replace("-", "")}.csv', index=False)
        main.to_csv(f'data/main.csv', index=False)
        print('Готово!!!')
    else:
        print('Даты постов прерываются')
        print(f'Максимальная собранных исторических данных: {max_dt}, Минимальная дата последней сборки: {min_dt_last}')
