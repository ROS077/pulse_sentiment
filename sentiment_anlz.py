# https://leftjoin.ru/all/constitution-sentiment-analysis/
# https://pypi.org/project/dostoevsky/

from dostoevsky.tokenization import RegexTokenizer
from dostoevsky.models import FastTextSocialNetworkModel
import pandas as pd



# Простым запросом содержимое всей таблицы с постами занесём в переменную vk_posts.
# Пройдём все посты, выберем те посты, где есть текст помимо пробелов и положим их в DataFrame.
df = pd.read_csv('data/main.csv')
df = df[df['ticker'] != 'USDRUB']

list_of_posts = []

for post in df.post:
    list_of_posts.append(str(post).replace("\n",""))
df_posts = df.copy()
df_posts['post_prepared'] = list_of_posts


# Обходим моделью весь список постов с текстом и получаем к оценку тональности для каждой записи.
tokenizer = RegexTokenizer()
model = FastTextSocialNetworkModel(tokenizer=tokenizer)
sentiment_list = []
results = model.predict(list_of_posts, k=2)
for sentiment in results:
    sentiment_list.append(sentiment)



neutral_list = []
negative_list = []
positive_list = []
speech_list = []
skip_list = []
for sentiment in sentiment_list:
    neutral = sentiment.get('neutral')
    negative = sentiment.get('negative')
    positive = sentiment.get('positive')
    if neutral is None:
        neutral_list.append(0)
    else:
        neutral_list.append(sentiment.get('neutral'))
    if negative is None:
        negative_list.append(0)
    else:
        negative_list.append(sentiment.get('negative'))
    if positive is None:
        positive_list.append(0)
    else:
        positive_list.append(sentiment.get('positive'))
df_posts['neutral'] = neutral_list
df_posts['negative'] = negative_list
df_posts['positive'] = positive_list
df_posts['total'] = df_posts['positive'] - df_posts['negative']/2
# df_posts['total'] = df_posts['positive'] - df_posts['negative'] + ((df_posts.neutral - df_posts.neutral.mean())/1)
# df_posts['total'] = (df_posts['positive'] / df_posts[df_posts['positive']>0].positive.mean()) - (df_posts['negative'] / df_posts[df_posts['negative']>0].negative.mean())
# df_posts['total'] = (df_posts['positive'] / df_posts[df_posts['positive']>0].positive.quantile(.5)) - (df_posts['negative'] / df_posts[df_posts['negative']>0].negative.quantile(.5))


main_agg_info = pd.DataFrame(df_posts.groupby(['posts_dt'])['negative', 'positive', 'total'].mean())
# main_agg_info.rolling(window=3).mean()
main_agg_info.rolling(window=3).mean().to_csv(f'days_result.csv', index=True)