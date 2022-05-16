import asyncio
from datetime import datetime
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import os, sys
import time
import pandas as pd
import numpy as np
import logging
import time
import threading
import websocket
import json


#sys.stdout = open(os.devnull, 'w')

symbol = 'MATIC-USD'

global last_time

schema = avro.schema.parse(open("timing_1.avsc", "r").read())
writer = DataFileWriter(open("timing_1.avro", "wb"), DatumWriter(), schema)
writer.append({"direction": float(os.getpid()), "direction_change_time": float(time.time())})
writer.close()

last_time = time.time()

# This is our callback function. For now, it just prints messages as they come.
def on_message(ws, message):
	try:
		message = json.loads(message)
		for data in message:
			try:

				if data.get('t') != None:
			
					pandas_storage_1 = np.array([])
					pandas_storage_1 = np.append(pandas_storage_1, [[data.get('t')], [symbol], [data.get('p')], [data.get('s')]])
					shape = int(len(pandas_storage_1)/4)
					if shape == 0:
						shape = 1
					pandas_storage_1 = pandas_storage_1.reshape(shape, 4)
					df_1 = pd.DataFrame(data=pandas_storage_1[0:, 0:], index=pandas_storage_1[0:, 0], columns=['Time', 'Symbol', 'Price', 'Volume'])
					df_1['Time'] = df_1['Time'].apply(lambda x: float(x)*1000000)
					df_1.reset_index(drop=True, inplace=True)
					last_time = float(df_1['Time'].iloc[-1])/1000000000
					df_1.to_csv('current_stream_' + symbol + '_1.csv', mode='a', header=False)
					pandas_storage_1 = np.array([])
					
			except Exception as e:
				pass
					
	except Exception as e:
		pass


def on_error(ws, error):
	print(error)

def on_close(ws):
	print("### closed ###")

def on_open(ws):
	ws.send('{"action":"auth","params":"*****"}')
	ws.send('{"action":"subscribe", "params":"XT.X:MATIC-USD"}')

#websocket.enableTrace(True)
ws = websocket.WebSocketApp("wss://socket.polygon.io/crypto",
							on_message = on_message,
							on_error = on_error,
							on_close = on_close)
ws.on_open = on_open
ws.run_forever()


