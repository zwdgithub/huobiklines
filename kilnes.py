# -*- coding: utf-8 -*-
import ssl
import gzip
import uuid
import json
import time
import StringIO
from websocket import create_connection

def createConnection(tradeId, symbol):
    print '{}, {} create connection'.format(tradeId, symbol)
    try:
        ws = create_connection("wss://api.huobipro.com/ws", sslopt={"cert_reqs": ssl.CERT_NONE})
        tradeStr = '{{"id": "{}", "sub": "market.{}.kline.1min"}}'
        ws.send(tradeStr.format(tradeId, symbol))
        return ws
    except Exception, e:
        time.sleep(10)
        return createConnection(tradeId, symbol)

def main(symbol):
    tradeId = uuid.uuid1()
    ws = createConnection(tradeId, symbol)
    while True:
        try:
            compressData = ws.recv()
            compressedStream = StringIO.StringIO(compressData)
            gzipFile = gzip.GzipFile(fileobj=compressedStream)
            result = gzipFile.read()
            if result[:7] == '{"ping"':
                ts = result[8:21]
                pong = '{"pong":' + ts + '}'
                ws.send(pong)
                print 'pong'
            else:
                print(result)
        except Exception as e:
            print('exception: {}'.format(e))
            ws = createConnection(tradeId, symbol)

def history(symbol):
    tradeId = uuid.uuid1()
    ws = createConnection()
    tradeStr = '{{"id": "{}", "req": "market.{}.kline.1min", "from": {}, "to": {}}}'
    start = 1511845180
    ws.send(tradeStr.format(tradeId, symbol, 1511845180, 1511845180+60*240))
    # sql = "insert into {} (id, open, close, low, high, amount, vol, symbol) values (%s, %s, %s, %s, %s, %s, %s, %s, '')"
    index = 1
    while True:
        try:
            compressData = ws.recv()
            compressedStream = StringIO.StringIO(compressData)
            gzipFile = gzip.GzipFile(fileobj=compressedStream)
            result = gzipFile.read()
            if result[:7] == '{"ping"':
                ts = result[8:21]
                pong = '{"pong":' + ts + '}'
                ws.send(pong)
                ws.send(tradeStr.format(tradeId, symbol, start+60*240*index+1, start+60*240*(index+1)))
            else:
                print(result)
                ws.send(tradeStr.format(tradeId, symbol, start+60*240*index+1, start+60*240*(index+1)))
            index += 1
            time.sleep(1)
        except Exception as e:
            print('exception: {}'.format(e))
            ws = createConnection()

if __name__ == '__main__':
    import threading
    thread_list = []
    symbols = ['btcusdt','bchusdt','ethusdt','etcusdt','ltcusdt','eosusdt','xrpusdt','omgusdt','dashusdt','zecusdt','adausdt','ctxcusdt','actusdt','btmusdt','btsusdt','ontusdt','iostusdt','htusdt','trxusdt','dtausdt','neousdt','qtumusdt','elausdt','venusdt','thetausdt','sntusdt','zilusdt','xemusdt','smtusdt','nasusdt','ruffusdt','hsrusdt','letusdt','mdsusdt','storjusdt','elfusdt','itcusdt','cvcusdt','gntusdt']
    for symbol in symbols:
        thread_list.append(threading.Thread(target=main, args=(symbol,)))
    for t in thread_list:
        t.start()
    for t in thread_list:
        t.join()