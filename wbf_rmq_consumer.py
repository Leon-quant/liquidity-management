# 消费者 consumer
import pika, time, dingding
import numpy as np
import ast
from influxdb import InfluxDBClient

influx_client = InfluxDBClient('0', '0', '', 'user', '')
tags = {}

# consumer = pika.BlockingConnection \
#     (pika.ConnectionParameters('127.0.0.1'))  # 创建socket连接

credentials = pika.PlainCredentials('user', 'password')  # mq用户名和密码
consumer = pika.BlockingConnection(
    pika.ConnectionParameters(host='127.0.0.1', port=0, credentials=credentials))  # 建立一个最基本的socket
channel = consumer.channel()  # 创建管道
channel.queue_declare(queue='wbf', durable=True)


def Getsymbolname(msg):
    ch = msg['channel']
    first = ch.index('_')
    last = ch.index('_',7)
    # print(first)
    # print(last)
    # print(channel[first+1:last])
    return ch[first+1:last]

def backcall(ch, method, properties, body):  # 参数body是发送过来的消息。
    # print(body)
    msg = eval(body)
    # print('receive a body: %r' % msg)

    #参数：
    num = 4
    slant_percentage5 = 0.05
    percentage5 = 0.05
    percentage10 = 0.1
    maxtime = 120
    N = 1


    #币对名
    symbol = Getsymbolname(msg)
    measurement = str(symbol)

    #计算 买卖相对价差
    bid1= msg['tick']['buys'][0][0]
    ask1= msg['tick']['asks'][0][0]
    # print(bid1)
    bids_num = len(msg['tick']['buys'])
    asks_num= len(msg['tick']['asks'])
    # print(ask1)
    mid_price = (bid1+ ask1) / 2
    relative_spread= (ask1 - bid1) / mid_price
    # print(relative_spread)

    # 计算流动性密度
    abc = msg['tick']['buys']
    bid_vol_num = 0
    for i in range(0, 5):
        bid_vol_num = abc[i][1] + bid_vol_num
    # print(bid_vol_num)
    cba = msg['tick']['asks']
    ask_vol_num = 0
    for i in range(0, 5):
        ask_vol_num = cba[i][1] + ask_vol_num
    # print(ask_vol_num)
    sum_vol = bid_vol_num+ ask_vol_num
    bid_min_price= abc[num][0]
    ask_max_price = cba[num][0]
    spread = ask_max_price - bid_min_price
    liquidity_density = sum_vol / spread
    # print(liquidity_density)

    # 计算倾斜指数
    up_price5 = mid_price * (1 + percentage5)
    low_price5 = mid_price * (1 - percentage5)
    up_price10 = mid_price * (1 + percentage10)
    low_price10 = mid_price * (1 - percentage10)

    # 5挡深度计算
    bid_vol5 = 0
    bid_amount5 = 0
    num_bid5 = 0
    for bid in msg['tick']['buys']:
        price = bid[0]
        vol = bid[1]
        #     print(price)
        #     print(vol)
        if float(price) > float(low_price5):
            bid_vol5 = bid_vol5 + float(vol)
            bid_amount5 = bid_amount5+ float(price) * float(vol)
            num_bid = num_bid5 + 1
    # print(bid_vol5)
    # print(bid_amount5)
    # print(num_bid5)
    # print('--------------------------------------------')
    ask_vol5 = 0
    ask_amount5 = 0
    num_ask5 = 0
    for ask in msg['tick']['asks']:
        price = ask[0]
        vol = ask[1]
        #     print(price)
        #     print(vol)
        if float(price) < float(up_price5):
            ask_vol5 = ask_vol5 + float(vol)
            ask_amount5 = float(ask_amount5) + float(price) * float(vol)
            num_ask = num_ask5 + 1
    # print(ask_vol5)
    # print(ask_amount5)
    # print(num_ask5)
    #     print('***********************************************************')
    try:
        slant_index = ask_vol5/ bid_vol5
    except:
        slant_index = 0
    # print(slant_index)
    sum_vol5 = bid_vol5+ ask_vol5
    # print(sum_vol)
    sum_amount5= bid_amount5+ ask_amount5
    # print(sum_amount)
    try:
        ave_price = sum_amount5 / sum_vol5
    except:
        ave_price = 0

    # 10挡深度计算
    bid_vol10 = 0
    bid_amount10 = 0
    num_bid10 = 0
    for bid in msg['tick']['buys']:
        price = bid[0]
        vol = bid[1]
        #     print(price)
        #     print(vol)
        if float(price) > float(low_price10):
            bid_vol10 = bid_vol10 + float(vol)
            bid_amount10 = bid_amount10+ float(price) * float(vol)
            num_bid10 = num_bid10 + 1
    # print(bid_vol10)
    # print(bid_amount10)
    # print(num_bid10)
    # print('--------------------------------------------')
    ask_vol10 = 0
    ask_amount10 = 0
    num_ask10 = 0
    for ask in msg['tick']['asks']:
        price = ask[0]
        vol = ask[1]
        #     print(price)
        #     print(vol)
        if float(price) < float(up_price10):
            ask_vol10 = ask_vol10 + float(vol)
            ask_amount10 = ask_amount10 + float(price) * float(vol)
            num_ask10 = num_ask10 + 1
    # print(ask_vol)
    # print(ask_amount)
    # print(num_ask10)
    #     print('***********************************************************')

    sum_vol10 = bid_vol10 + ask_vol10
    # print(sum_vol)
    sum_amount10 = bid_amount10 + ask_amount10
    # print(sum_amount)
    ave_price = sum_amount10/ sum_vol10


    if bids_num < 2:
        dingding.send_msg(
            url='https://oapi.dingtalk.com/robot/send?access_token=',
            reminders=[], msg=str(symbol) + '买盘已空,当前：' + str(bids_num),
            isAtAll=False)
    if asks_num < 2:
        dingding.send_msg(
            url='https://oapi.dingtalk.com/robot/send?access_token=',
            reminders=[], msg=str(symbol) + '卖盘已空,当前：' + str(asks_num),
            isAtAll=False)

    content = {"Symbol":symbol ,"Time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), "Relative_spread": relative_spread,
                     "liqudity_density": liquidity_density, "Slant_index": slant_index, "Depth_5%_bid": bid_amount5, "Depth_5%_ask": ask_amount5,
                     "Depth_5%_sum": sum_amount5, "Depth_10%_bid": bid_amount10,
                     "Depth_10%_ask": ask_amount10,
                     "Depth_10%_sum": sum_amount10}

    print(content)

    tags["data"] = content

    fields = {}
    fields = content

    json_body = [
        {
            "measurement": measurement,
            "tags": tags,
            "fields": fields
        }
    ]
    influx_client.write_points(json_body)




channel.basic_consume('wbf',  # 队列名
                      backcall,  # 回调函数。执行结束后立即执行另外一个函数返回给发送端是否执行完毕。
                      True)  # 不会告知服务端我是否收到消息。一般注释。如果注释掉，对方没有收到消息的话不会将消息丢失，始终在队列里等待下次发送。

print('waiting for message To exit   press CTRL+C')
channel.start_consuming()  # 启动后进入死循环。一直等待消息。



