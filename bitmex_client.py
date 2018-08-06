import requests
import hmac
import hashlib
from time import time, sleep
from numpy import floor
import urllib
import json, simplejson
import socket
import datetime
from bitmex import bitmex
import traceback

class BitmexClient(object):

    def __init__(self, account, test = True,):
        if account == 'triple0':
            self.api_key = 'mcu708dzcnwGcWEbbv0u_7OP'
            self.secret = 'zZ9JFTd5BVjwIRGGRhXM2F9WY3Ppya0yK9f_ZChuZLiI0_3v'#.encode('utf-8')
        elif account == 'throwaway':
            self.api_key = 'c75tJGd2yzj1XYgCIxu5fk1n'
            self.secret = 'iHbrb9C_Or2KzBeYHcxLg1rA8L9ES9CkNSHaSJBJ2FL5927n'#.encode('utf-8')
        elif account == 'patdra':
            self.api_key = 'Qje-eytafaWXAMMAIED7s2nJ'
            self.secret = 'qyOt710wLQ9HUFJ4DAENvJ6Gjxhjw-YOHaBZpzxT3LiQxCe_'#.encode('utf-8')
        elif account == 'infinite':
            self.api_key = 'nKmA36DnCLvY1IKdYACdcIV4'
            self.secret = 'HjxKB5xERiyS6npozcztRd5QQdce7SF9gRpYY1ytpQZFd7xF'#.encode('utf-8')
        elif account == 'infRead':
            self.api_key = 'Tk4ANMd6SCgvhh4pckKX1jmZ'
            self.secret = 'T2Lwpx0JOWOwXPSm1shK2dpVxvidRbnr9zxcW2isv5tQ-f9J'#.encode('utf-8')
        else:
            self.api_key = None
            self.secret = None
        if not(test):
            self.URI = 'https://bitmex.com'
            self.bit = bitmex(test = False, api_key = self.api_key, api_secret = self.secret)
        else:
            self.URI = 'https://testnet.bitmex.com'
            self.bit = bitmex(test = True, api_key = self.api_key, api_secret = self.secret)
        if self.secret:
            self.secret = self.secret.encode('utf-8')
        self.reset = 0
        self.servTime = 0

        self.testNum = 0
        self.retries = 0

        
    def __get_data(self, verb, path, d):
        self.testNum = 1
        t = floor(time() + 1)
        if self.reset > t:
            diff = self.reset - t
            if diff > 1:
                print('Sleeping for', diff, 'seconds')
            sleep(diff)

        
        expires = str(round(time() + 30))
        message = [verb, path, expires, d]
        message = ''.join(message).encode('utf-8')
        if self.secret:
            signature = hmac.new(self.secret, message, hashlib.sha256).hexdigest()
            headers = {"Content-Type":"application/json", "api-key":self.api_key, "api-signature":signature, "api-expires":expires}
        else:
            headers = {"Content-Type":"application/json"}
        
        URL = [self.URI, path]
        URL = ''.join(URL)

        #req = requests.Request('GET', URL, headers = headers)
        #print(req.url)
        #print(req.headers)

        try:
            if verb == 'GET':
                if d != '':
                    r = requests.get(URL , headers = headers, params = d)
                else:
                    r = requests.get(URL, headers = headers)
            elif verb == 'POST':
                if d != '':
                    r = requests.post(URL, headers = headers, data = d)
                else:
                    r = requests.post(URL, headers = headers)
            elif verb == 'DELETE':
                if d!= '':
                    r = requests.delete(URL, headers = headers, data = d)                
                else:
                    r = requests.delete(URL, headers = headers)
            status = r.status_code
            result = r.json()
            if 'x-ratelimit-reset' in r.headers:
                self.reset = int(r.headers['x-ratelimit-reset'])
            if 'date' in headers:
                self.servTime = datetime.datetime.strptime(headers['date'], '%a, %d %b %Y %H:%M:%S %Z')
                self.servTime = self.servTime.replace(tzinfo=datetime.timezone.utc).timestamp()
            if 'x-ratelimit-reset' in headers and 'date' in headers:
                diff = self.servTime - self.reset
                self.reset = time() + diff
        except socket.gaierror:
            print('Its broken. Retrying...', URL, headers, d)
            return self.__get_data(verb, path, d)
        except simplejson.errors.JSONDecodeError:
            #print('JSON broken. ', r)
            pass
        except TimeoutError:
            return self.__get_data(verb, path, d)

        if status == 200:
            return status, result
        elif status == 429:
            if int(r.headers['Retry-After']) > 1:
                print('Max rate exceeded. Sleeping...', r.headers['Retry-After'])
            sleep(int(r.headers['Retry-After']))
            return self.__get_data(verb, path, d)
        elif status == 502:
            #print('Bad Gateway: retrying after 1s...')
            sleep(1)
            return self.__get_data(verb, path, d)
        elif status == 524:
            #print('A Timeout Occurred: retrying...')
            return self.__get_data(verb, path, d)
        else:
            if not('Account has insufficient Available Balance' in result['error']['message']):
                #print('Error: ', status)
                print(result['error']['message'])
                return status, result['error']['message']
            else:
                #print(result['error']['message'])
                return status, result['error']['message']

    def __get_data2(self, r):
        self.testNum = 2
        headers = r[1].headers
        if 'x-ratelimit-reset' in headers:
            self.reset = int(headers['x-ratelimit-reset'])
        if 'date' in headers:
            self.servTime = datetime.datetime.strptime(headers['date'], '%a, %d %b %Y %H:%M:%S %Z')
            self.servTime = self.servTime.replace(tzinfo=datetime.timezone.utc).timestamp()
        if 'x-ratelimit-reset' in headers and 'date' in headers:
            diff = self.servTime - self.reset
            self.reset = time() + diff
        status = r[1].status_code
        result = r[0]


        status, result = self.__use_status(status, result, headers)

        return status, result

    def __use_status(self, status, result, headers):
        if status == 200:
            return status, result
        elif status == 429:
            if int(headers['Retry-After']) > 1:
                print('Max rate exceeded. Sleeping...', r.headers['Retry-After'])
            sleep(int(headers['Retry-After']))
            return -1, -1
        elif status == 502:
            #print('Bad Gateway: retrying after 1s...')
            sleep(1)
            return -1, -1
        elif status == 524:
            #print('A Timeout Occurred: retrying...')
            return -1, -1
        else:
            if not('Account has insufficient Available Balance' in result['error']['message']):
                #print('Error: ', status)
                print(result['error']['message'])
                return status, result['error']['message']
            else:
                #print(result['error']['message'])
                return status, result['error']['message']


    def get_position(self):
        verb = 'GET'
        path = '/api/v1/position'
        data = ''

        status, data = self.__get_data(verb, path, data)
        
        if len(data):
            return data[0]['currentQty']
        else:
            return 0

    def get_wallet(self):
        verb = 'GET'
        path = '/api/v1/user/margin'
        data = ''

        status, data = self.__get_data(verb, path, data)

        balance = data['marginBalance']
        available = data['availableMargin']
        return balance, available

    def get_balance(self):
        verb = 'GET'
        path = '/api/v1/user/margin'
        data = ''

        status, data = self.__get_data(verb, path, data)

        balance = data['amount']
        return balance


    def get_orders(self):
        '''verb = 'GET'
        path = '/api/v1/order?filter=%7B%22open%22%3A%22true%22%7D'
        data = ''

        status, data = self.__get_data(verb, path, data)

        price = []
        isBot = []
        size = []
        for i in data:
            price.append(i['price'])
            if i['text'] == 'bot':
                isBot.append(1)
            else:
                isBot.append(0)
            if i['side'] == 'Buy':
                size.append(i['orderQty'])
            elif i['side'] == 'Sell':
                size.append(-i['orderQty'])
        diction = {"price": price, "isBot":isBot, "size":size}'''
        
        t = floor(time() + 1)
        if self.reset > t:
            diff = self.reset - t
            if diff > 1:
                print('Sleeping for', diff, 'seconds')
            sleep(diff)
        while True:
            try:
                r = self.bit.Order.Order_getOrders().result()
                status, data = self.__get_data2(r)
                if status == -1:
                    self.retries += 1
                    pass
                else:
                    self.retries = 0
                    break
            except Exception as e:
                self.retries += 1
                if self.retries > 100:
                    print('100 tries exceeded:',e)
                    raise e
                if 'Connection aborted' in str(e) or 'bad handshake' in str(e) or 'Connection broken' in str(e) or '524' in str(e):
                    pass
                else:
                    print('From get_orders:', e)
        price = []
        isBot = []
        size = []
        for i in data:
            price.append(i['price'])
            if i['text'] == 'bot':
                isBot.append(1)
            else:
                isBot.append(0)
            if i['side'] == 'Buy':
                size.append(i['orderQty'])
            elif i['side'] == 'Sell':
                size.append(-i['orderQty'])

        data = {"price": price, "isBot":isBot, "size":size}

        return data

    def post_order(self, size, price = 0):
        '''verb = 'POST'
        path = '/api/v1/order'

        if price:
            data = {"symbol":"XBTUSD", "orderQty":size, "price":price}
        else:
            data = {"symbol":"XBTUSD", "orderQty":size, "ordType":"Market", "text":"bot"}
        data = json.dumps(data, separators=(',',':'))

        status, data = self.__get_data(verb, path, data)'''
        t = floor(time() + 1)
        if self.reset > t:
            diff = self.reset - t
            if diff > 1:
                print('Sleeping for', diff, 'seconds')
            sleep(diff)
        while True:
            try:
                if price:
                    r = self.bit.Order.Order_new(symbol='XBTUSD', orderQty=size, price=price).result()
                else:
                    r = self.bit.Order.Order_new(symbol='XBTUSD', orderQty=size, ordType='Market').result()
                status, data = self.__get_data2(r)
                if status == -1:
                    self.retries += 1
                    pass
                else:
                    self.retries = 0
                    break
            except Exception as e:
                self.retries += 1
                if self.retries > 100:
                    print('100 tries exceeded:',e)
                    raise e
                if 'Connection aborted' in str(e) or 'bad handshake' in str(e) or 'Connection broken' in str(e) or '524' in str(e):
                    pass
                else:
                    print('From post_order:', e)

        return status, data

    def post_simple_order(self, size, price = 0):
        '''verb = 'POST'
        path = '/api/v1/order'

        if price:
            data = {"symbol":"XBTUSD", "simpleOrderQty":size, "price":price, "text":"bot"}
        else:
            data = {"symbol":"XBTUSD", "simpleOrderQty":size, "ordType":"Market", "text":"bot"}
        data = json.dumps(data, separators=(',',':'))

        status, data = self.__get_data(verb, path, data)'''

        t = floor(time() + 1)
        if self.reset > t:
            diff = self.reset - t
            if diff > 1:
                print('Sleeping for', diff, 'seconds')
            sleep(diff)
        while True:
            try:
                if price:
                    r = self.bit.Order.Order_new(symbol='XBTUSD', simpleOrderQty=size, price=price).result()
                else:
                    r = self.bit.Order.Order_new(symbol='XBTUSD', simpleOrderQty=size, ordType='Market').result()
                status, data = self.__get_data2(r)
                if status == -1:
                    self.retries += 1
                    pass
                else:
                    self.retries = 0
                    break
            except Exception as e:
                self.retries += 1
                if self.retries > 100:
                    print('100 tries exceeded:',e)
                    raise e
                if 'Connection aborted' in str(e) or 'bad handshake' in str(e) or 'Connection broken' in str(e) or '524' in str(e):
                    pass
                else:
                    print('From post_simple_order:', e)

        return status, data
        

    def delete_orders(self):
        '''verb = 'DELETE'
        path = '/api/v1/order/all'
        data = ''

        status, data = self.__get_data(verb, path, data)'''

        t = floor(time() + 1)
        if self.reset > t:
            diff = self.reset - t
            if diff > 1:
                print('Sleeping for', diff, 'seconds')
            sleep(diff)
        while True:
            try:
                r = self.bit.Order.Order_cancelAll().result()
                status, data = self.__get_data2(r)
                if status == -1:
                    self.retries += 1
                    pass
                else:
                    self.retries = 0
                    break
            except Exception as e:
                self.retries += 1
                if self.retries > 100:
                    print('100 tries exceeded:',e)
                    raise e
                if 'Connection aborted' in str(e) or 'bad handshake' in str(e) or 'Connection broken' in str(e) or '524' in str(e):
                    pass
                else:
                    print('From delete_orders:', e)

        return status, data

    def post_leverage(self, leverage):
        '''verb = 'POST'
        path = '/api/v1/position/leverage'
        data = {'symbol':'XBTUSD', 'leverage':leverage}
        data = json.dumps(data, separators=(',',':'))

        status, data = self.__get_data(verb, path, data)'''

        t = floor(time() + 1)
        if self.reset > t:
            diff = self.reset - t
            if diff > 1:
                print('Sleeping for', diff, 'seconds')
            sleep(diff)
        while True:
            try:
                r = self.bit.Position.Position_updateLeverage(symbol='XBTUSD', leverage = leverage).result()
                status, data = self.__get_data2(r)
                if status == -1:
                    self.retries += 1
                    pass
                else:
                    self.retries = 0
                    break
            except Exception as e:
                self.retries += 1
                if self.retries > 100:
                    print('100 tries exceeded:',e)
                    raise e
                if 'Connection aborted' in str(e) or 'bad handshake' in str(e):
                    pass
                else:
                    print('From post_leverage:', e)
        
        return status, data

    def get_orderbook(self, depth):
        '''verb = 'GET'
        path = ['/api/v1/orderBook/L2?symbol=XBTUSD&depth=',str(depth)]
        path = ''.join(path)
        data = ''

        status, data = self.__get_data(verb, path, data)'''

        t = floor(time() + 1)
        if self.reset > t:
            diff = self.reset - t
            if diff > 1:
                print('Sleeping for', diff, 'seconds')
            sleep(diff)
        while True:
            try:
                r = self.bit.OrderBook.OrderBook_getL2(symbol = 'XBTUSD', depth = depth).result()
                status, data = self.__get_data2(r)
                if status == -1:
                    self.retries += 1
                    pass
                else:
                    self.retries = 0
                    break
            except Exception as e:
                self.retries += 1
                if self.retries > 100:
                    print('100 tries exceeded:',e)
                    raise e
                if 'Connection aborted' in str(e) or 'bad handshake' in str(e) or 'Connection broken' in str(e) or '524' in str(e):
                    pass
                else:
                    print('From get_orders:', e)
        return data

    def get_recent_trades(self, count, ts):
        if ts >= 1 and ts < 5:
            bs = '1m'
        elif ts >= 5 and ts < 60:
            bs = '5m'
        elif ts >= 60 and ts < 60*24:
            bs = '1h'
        elif ts >= 60*24:
            bs = '1d'
        else:
            bs = '1h'
        
        '''verb = 'GET'
        path = ['/api/v1/trade/bucketed?symbol=XBTUSD&reverse=true&count=',str(count),'&binSize=',bs]
        path = ''.join(path)
        data = ''

        status, data = self.__get_data(verb, path, data)
        return data'''
        t = floor(time() + 1)
        if self.reset > t:
            diff = self.reset - t
            if diff > 1:
                print('Sleeping for', diff, 'seconds')
            sleep(diff)
        while True:
            try:
                r = self.bit.Trade.Trade_getBucketed(reverse = True, symbol = 'XBTUSD', count = count, binSize = bs).result()
                status, data = self.__get_data2(r)
                if status == -1:
                    self.retries += 1
                    pass
                else:
                    self.retries = 0
                    break
            except Exception as e:
                self.retries += 1
                if self.retries > 100:
                    print('100 tries exceeded:',e)
                    raise e
                if 'Connection aborted' in str(e) or 'bad handshake' in str(e) or 'Connection broken' in str(e) or '524' in str(e):
                    pass
                else:
                    print('From get_orders:', e)
        return data

    def get_trade(self):
        '''verb = 'GET'
        path = '/trade?symbol=XBTUSD&reverse=true&count=1'
        data = ''

        status, data = self.__get_data(verb, path, data)
        return data'''

        t = floor(time() + 1)
        if self.reset > t:
            diff = self.reset - t
            if diff > 1:
                print('Sleeping for', diff, 'seconds')
            sleep(diff)
        while True:
            try:
                r = self.bit.Trade.Trade_get(symbol = 'XBTUSD', reverse = True, count = 1).result()
                status, data = self.__get_data2(r)
                if status == -1:
                    self.retries += 1
                    pass
                else:
                    self.retries = 0
                    break
            except Exception as e:
                self.retries += 1
                if self.retries > 100:
                    print('100 tries exceeded:',e)
                    raise e
                if 'Connection aborted' in str(e) or 'bad handshake' in str(e) or 'Connection broken' in str(e) or '524' in str(e):
                    pass
                else:
                    print('From get_trade:', e)
        return data

    def get_historic_trades(self, binSize, startTime, endTime):
        '''startTime = startTime.isoformat()
        endTime = endTime.isoformat()
        
        verb = 'GET'
        path = ['/api/v1/trade/bucketed?symbol=XBTUSD&count=500&startTime=',startTime,'&endTime=',endTime,'&binSize=',binSize]
        path = ''.join(path)
        data = ''

        status, data = self.__get_data(verb, path, data)
        return data'''

        t = floor(time() + 1)
        if self.reset > t:
            diff = self.reset - t
            if diff > 1:
                print('Sleeping for', diff, 'seconds')
            sleep(diff)
        while True:
            try:
                r = self.bit.Trade.Trade_getBucketed(symbol = 'XBTUSD', count = 500, startTime = startTime, endTime = endTime, binSize = binSize).result()
                status, data = self.__get_data2(r)
                if status == -1:
                    self.retries += 1
                    pass
                else:
                    self.retries = 0
                    break
            except Exception as e:
                self.retries += 1
                if self.retries > 100:
                    print('100 tries exceeded:',e)
                    raise e
                if 'Connection aborted' in str(e) or 'bad handshake' in str(e) or 'Connection broken' in str(e) or '524' in str(e):
                    pass
                else:
                    print('From get_historic_trades:', e)
                    traceback.print_tb(e.__traceback__)
        return data
        

    def get_historic_funding(self, startTime, endTime):
        '''startTime = startTime.isoformat()
        endTime = endTime.isoformat()
        
        verb = 'GET'
        path = ['/api/v1/funding?symbol=XBTUSD&count=500&startTime=',startTime,'&endTime=',endTime]
        path = ''.join(path)
        data = ''

        status, data = self.__get_data(verb, path, data)
        return data'''

        t = floor(time() + 1)
        if self.reset > t:
            diff = self.reset - t
            if diff > 1:
                print('Sleeping for', diff, 'seconds')
            sleep(diff)
        while True:
            try:
                r = self.bit.Funding.Funding_get(symbol = 'XBTUSD', count = 500, startTime = startTime, endTime = endTime).result()
                status, data = self.__get_data2(r)
                if status == -1:
                    self.retries += 1
                    pass
                else:
                    self.retries = 0
                    break
            except Exception as e:
                self.retries += 1
                if self.retries > 100:
                    print('100 tries exceeded:',e)
                    raise e
                if 'Connection aborted' in str(e) or 'bad handshake' in str(e) or 'Connection broken' in str(e) or '524' in str(e):
                    pass
                else:
                    print('From get_historic_funding:', e)
        return data
