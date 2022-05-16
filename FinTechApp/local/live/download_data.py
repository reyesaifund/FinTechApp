#!/Users/reyestrading/.pyenv/versions/pyenv_36

import json
import requests
from datetime import datetime, timedelta
import numpy as np
import time
import pandas as pd
import os, sys
import pytz
import glob

np.set_printoptions(threshold=sys.maxsize)

symbol = 'MATIC-USD'

os.system('current_stream_MATIC-USD_1.csv')


start_time = time.time()

start_time_link = (time.time()-(60*60*24*2))*1000000000

while True:

    try:

        end_time = time.time()

        current_day = datetime.utcfromtimestamp(start_time_link/1000000000).strftime('%Y-%m-%d')
        

        print('start_date')
        print(current_day)
        print(datetime.utcfromtimestamp(start_time_link/1000000000))
        print((end_time-start_time)/60)

        #link = 'https://api.polygon.io/v3/trades/X:MATIC-USD?timestamp=' + str(int(start_time_link)) + '&limit=50000&sort=timestamp&apiKey=******'
        link = 'https://api.polygon.io/v1/historic/crypto/MATIC/USD/' + current_day + '?offset=' + str(int(start_time_link)) + '&limit=10000&apiKey=*****'
        price = np.array([])
        tick_data = requests.get(link).json()
        print(tick_data)
        tick_data = tick_data.get('ticks')
        if tick_data is not None:
            price = np.array([])
            for x2 in tick_data:
                price = np.append(price, np.array([[x2.get('t')], [symbol], [x2.get('p')], [x2.get('s')]], dtype=object))	
            if price.size > 0:
                shape = len(price)/4
                price = price.reshape((int(shape), 4))
                price_pd = pd.DataFrame(data=price[0:, 0:], index=price[0:, 0], columns=['Time', 'Symbol', 'Price', 'Volume'])
                price_pd.drop_duplicates(keep='first', inplace=True)
                #price_pd['Time'] = price_pd['Time'].apply(lambda x: x * 1000000000)
                price_pd.set_index('Time', inplace=True)
                price_pd.sort_index(inplace=True)
                price_pd[~price_pd.index.duplicated(keep='first')]
                price_pd = price_pd.reset_index()
                print('LEN')
                print(len(price_pd))
                print(price_pd.head(3))
                if len(price_pd) == 1:
                    break
                start_time_link = float(price_pd['Time'].iloc[-1])
                #current_date = datetime.utcfromtimestamp(last_time_1/1000000000)
                file_found = 0
                current_py_files = glob.glob('*.csv')
                for py1_0 in current_py_files:
                    if py1_0 == 'current_stream_MATIC-USD_1.csv':	
                        file_found = 1
                if file_found == 1:
                    price_pd.to_csv('current_stream_MATIC-USD_1.csv', mode='a', header=False)
                else:
                    price_pd.to_csv('current_stream_MATIC-USD_1.csv', mode='w', header=False)
        else:
            break
                
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        print(e)

    
