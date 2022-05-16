#!/Users/reyestrading/.pyenv/versions/pyenv_37

from datetime import datetime, timedelta
import numpy as np
import time
import pandas as pd
import os, sys
import pytz
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import glob
import gc
import warnings

symbol = 'MATIC-USD'

schema = avro.schema.parse(open("trend_switch.avsc", "r").read())
writer = DataFileWriter(open('trend_switch_' + symbol + '.avro', "wb"), DatumWriter(), schema)
writer.append({"entry_time": 0, "entry_price": 0, "entry_direction": 0})
writer.close()

schema = avro.schema.parse(open("next_jump.avsc", "r").read())
writer = DataFileWriter(open('next_jump_' + symbol + '.avro', "wb"), DatumWriter(), schema)
writer.append({"last_jump_time": 0, "current_jump_time": 0, "next_jump_time": 0})
writer.close()

schema = avro.schema.parse(open("next_jump.avsc", "r").read())
writer = DataFileWriter(open('check_jumps_' + symbol + '.avro', "wb"), DatumWriter(), schema)
writer.append({"last_jump_time": 0, "current_jump_time": 0, "next_jump_time": 0})
writer.close()

schema = avro.schema.parse(open("start_time.avsc", "r").read())
writer = DataFileWriter(open('start_time' + symbol + '.avro', "wb"), DatumWriter(), schema)
writer.append({"start_time": 0})
writer.close()

schema = avro.schema.parse(open("hat_direction_2.avsc", "r").read())
writer = DataFileWriter(open('hat_direction_' + symbol + '.avro', "wb"), DatumWriter(), schema)
writer.append({"hat_direction": 0})
writer.close()

schema = avro.schema.parse(open("total_trades.avsc", "r").read())
writer = DataFileWriter(open('total_trades_' + symbol + '.avro', "wb"), DatumWriter(), schema)
writer.append({"time": 0, "side": 0, "entry_price": 0, "exit_price": 0, "avg_roc_ask": 0, "avg_roc_bid": 0})
writer.close()

schema = avro.schema.parse(open("total_trades.avsc", "r").read())
writer = DataFileWriter(open('total_trades_' + symbol + '_2.avro', "wb"), DatumWriter(), schema)
writer.append({"time": 0, "side": 0, "entry_price": 0, "exit_price": 0, "avg_roc_ask": 0, "avg_roc_bid": 0})
writer.close()

schema = avro.schema.parse(open("order_queue.avsc", "r").read())
writer = DataFileWriter(open('order_queue_' + symbol + '.avro', "wb"), DatumWriter(), schema)
writer.append({'queued_order_direction': 0})
writer.close()

#schema = avro.schema.parse(open("trailing.avsc", "r").read())
#writer = DataFileWriter(open('trailing_' + symbol + '.avro', "wb"), DatumWriter(), schema)
#writer.append({"trailing": 0})
#writer.close()

schema = avro.schema.parse(open("jump_timeout.avsc", "r").read())
writer = DataFileWriter(open('jump_timeout_' + symbol + '.avro', "wb"), DatumWriter(), schema)
writer.append({"lx_run_time": 0, "current_jump_time_1": 0, "next_jump_time_1": 0})
writer.close()