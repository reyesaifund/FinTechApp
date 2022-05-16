import json
import requests
from datetime import datetime, timedelta
import numpy as np
import time
import pandas as pd
import os, sys
import pytz
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import signal
import shutil
import ast
import math
import glob
from pathlib import Path
import subprocess
import gc
import warnings
import psutil

symbol = 'MATIC-USD'

while True:
	try:
		reader = DataFileReader(open('is_running_' + symbol + '.avro',"rb"), DatumReader())
		for r in reader:
			pid_system_time_2 = r.get('time')
		reader.close()
		
		time_since_system_start = time.time()-pid_system_time_2

		if time_since_system_start > 60*3:
			os.system("pkill -f streaming_polygon_MATIC.py")
			os.system("pkill -f MATIC_live_2.py")
			os.system("caffeinate python streaming_polygon_MATIC.py &")
			os.system("caffeinate python MATIC_live_2.py &")
		time.sleep(60)

	except Exception as e:
		pass

