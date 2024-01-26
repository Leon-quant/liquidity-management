# -*- coding: utf-8 -*-
"""
Created on Thu Feb 20 17:02:22 2020

@author: Administrator
"""
import pika
import websocket
import json
import zlib
import math
import time
import datetime
import pandas as pd
from influxdb import InfluxDBClient
import gzip

pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_row', None)
pd.set_option('display.max_column', None)
import dingding
import threading
import time
import sys
import numpy as np


# symbols = ['btcwt', 'ethwt', 'wptusdt', 'wptdusd', 'wptwt','dashusdt', 'dashdusd', 'xzcusdt','xzcdusd', 'zecusdt',
#            'zecdusd', 'xrpdusd','xlmusdt', 'xlmwt', 'trxwt', 'xmrusdt', 'xmrwt','neousdt','neowt', 'xrpwt',
#            'whcusdt','whcdusd','dogeusdt','dogedusd','btcvusdt','btcvdusd','btcvbtc','ontusdt','ontdusd','xemusdt','xemusdt']

def on_open(ws):
    print('on open')
    # {"event": "sub","params": {"channel": "market_$base$quote_depth_step[0-2]", "cb_id": "自定义", "asks": 150, "bids": 150}}
    ws.send(json.dumps(send_data[0]))  # btcwt
    ws.send(json.dumps(send_data[1]))  #ethwt
    ws.send(json.dumps(send_data[2]))  #wptusdt
    ws.send(json.dumps(send_data[3]))  #wptdusd
    ws.send(json.dumps(send_data[4]))  #wptwt
    ws.send(json.dumps(send_data[5]))  #dashusdt
    ws.send(json.dumps(send_data[6]))  #dashdusd
    ws.send(json.dumps(send_data[7]))  #xzcusdt
    ws.send(json.dumps(send_data[8]))  #xzcdusd
    ws.send(json.dumps(send_data[9]))  #zecusdt
    ws.send(json.dumps(send_data[10])) #zecdusd
    ws.send(json.dumps(send_data[11])) #xrpdusd
    ws.send(json.dumps(send_data[12])) #xlmusdt
    ws.send(json.dumps(send_data[13])) #xlmwt
    ws.send(json.dumps(send_data[14])) #trxwt
    ws.send(json.dumps(send_data[15])) #xmrusdt
    ws.send(json.dumps(send_data[16])) #xmrwt
    ws.send(json.dumps(send_data[17])) #neousdt
    ws.send(json.dumps(send_data[18])) #neowt
    ws.send(json.dumps(send_data[19])) #xrpwt
    ws.send(json.dumps(send_data[20])) #whcusdt
    ws.send(json.dumps(send_data[21])) #whcdusd
    ws.send(json.dumps(send_data[22])) #dogeusdt
    ws.send(json.dumps(send_data[23])) #dogedusd
    ws.send(json.dumps(send_data[24])) #btcvusdt
    ws.send(json.dumps(send_data[25])) #btcvdusd
    ws.send(json.dumps(send_data[26])) #btcvbtc
    ws.send(json.dumps(send_data[27])) #ontusdt
    ws.send(json.dumps(send_data[28])) #ontdusd
    ws.send(json.dumps(send_data[29])) #xemusdt
    ws.send(json.dumps(send_data[30])) #xemdusd



def on_message(ws, msg):
    # print('on message')
    # print(msg)
    msg = eval(gzip.decompress(msg).decode("utf-8"))
    # print(type(msg))
    # print(msg)

    if 'ping' in msg:
        ws.send(json.dumps({'pong': msg['ping']}))

    if 'channel' in msg:
        channel = msg.get('channel')
        # print(channel)
        credentials = pika.PlainCredentials('[user]', '[password]')  # mq用户名和密码
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='127.0.0.1',port=0,credentials = credentials))  # 建立一个最基本的socket
        chanel = connection.channel()  # 声明一个管道
        chanel.queue_declare(queue='wbf', durable=True)  # 给管道创建一个队列，参数是管道队列名。

        chanel.basic_publish(exchange='',
                             routing_key='wbf',
                             body=str(msg))  # 要发送的消息。

        print('发出一个消息: %r' % msg)
        connection.close()  # 关闭



def on_error(ws, error):
    print('on error')
    sys.exit()


def on_close(ws):
    print('on close')
    sys.exit()


if __name__ == '__main__':
    ws_url = 'wss://ws.wbf.live/kline-api/ws'
    send_data = []
    symbols = ['btcwt', 'ethwt', 'wptusdt', 'wptdusd', 'wptwt', 'dashusdt', 'dashdusd', 'xzcusdt', 'xzcdusd', 'zecusdt',
               'zecdusd', 'xrpdusd', 'xlmusdt', 'xlmwt', 'trxwt', 'xmrusdt', 'xmrwt', 'neousdt', 'neowt', 'xrpwt','whcusdt',
               # 'whcdusd',
               'dogeusdt', 'dogedusd', 'btcvusdt', 'btcvdusd', 'btcvbtc', 'ontusdt', 'ontdusd',
               'xemusdt', 'xemdusd']

    for n in range(0, len(symbols)):
        # print(symbols[n])
        a = symbols[n]
        channel_depth = "market_" + a + "_depth_step0"
        # channel_trade = "market_" + a + "_trade_ticker"

        data_depth = {"event": "sub", "params": {"channel": channel_depth, "cb_id": "原路返回"}}

        send_data.append(data_depth)
    print(send_data)
    # [{'event': 'sub', 'params': {'channel': 'market_btcwt_depth_step0', 'cb_id': '原路返回'}},
    #  {'event': 'sub', 'params': {'channel': 'market_ethwt_depth_step0', 'cb_id': '原路返回'}},
    #  {'event': 'sub', 'params': {'channel': 'market_wptusdt_depth_step0', 'cb_id': '原路返回'}},
    #   ... ...

    ws = websocket.WebSocketApp(ws_url, on_open=on_open,
                                on_message=on_message, on_error=on_error,
                                on_close=on_close)
    ws.run_forever(ping_interval=15)

    pass
