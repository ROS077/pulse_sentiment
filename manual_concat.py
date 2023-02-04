import pandas as pd
import datetime

today = datetime.datetime.today().strftime('%Y-%m-%d')
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

# not_full_data = last_posts_df[last_posts_df['posts_dt'] == min_df_last_posts].ticker.unique()
min_dt_for_ticks = pd.DataFrame(last_posts_df.groupby('ticker').posts_dt.min().sort_values()).reset_index()
not_full_data = min_dt_for_ticks[min_dt_for_ticks['posts_dt'] >= max_dt].ticker.values
print('Перечень тиков с неполным набором данных за последний период') # актуально в случае неуспеха основного скрипта
print(not_full_data)

if max_dt < min_df_last_posts:
    # lst_posts = last_posts_df[(last_posts_df['posts_dt'] > max_dt)
    #                         & (last_posts_df['posts_dt'] <= max_df_last_posts)]
    lst_posts = last_posts_df[(last_posts_df['posts_dt'] > max_dt)]
    main = pd.concat([main_df, lst_posts], ignore_index=True)
    main.to_csv(f'data/backup/main_{today.replace("-", "")}.csv', index=False)
    main.to_csv(f'data/main.csv', index=False)
    print('Готово!!!')