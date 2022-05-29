import pybithumb
import time
import datetime

'''
가상화폐 티커 목록 얻기
거래소에서 가상화페에 고유한 이름을 붙이는데 그것이 티커(ticker)다
get_tickers()로 티커 목록을 얻을 수 있다.
'''
tickers = pybithumb.get_tickers()
print(tickers)

'''
60초 동안 현재가 얻기
빗썸은 초당 20회가 넘어가면 차단된다.
따라서 sleep() 함수를 사용해서 이를 방지한다.
'''
price = 0
for i in range(2):
  price = pybithumb.get_current_price("BTC")
  print(price)
  time.sleep(0.5)

'''
get_market_detail() 함수는 [저가, 고가, 평균거래금액, 거래량]을 튜플로 리턴한다.
'''
detail = pybithumb.get_market_detail("BTC")
print(detail)

'''
호가
호가에 대한 자세한 설명은 다음을 참조
https://wikidocs.net/21881
'''
orderbook = pybithumb.get_orderbook("BTC")
print(orderbook)

ms = int(orderbook["timestamp"])  # 호가를 조회한 시간

dt = datetime.datetime.fromtimestamp(ms / 1000)
print(dt)


bids = orderbook['bids']
for bid in bids:
  price = bid['price']
  quant = bid['quantity']
  print("매수호가: ", price, "매수잔량: ", quant)

asks = orderbook['asks']
for ask in asks:
  price = ask['price']
  quant = ask['quantity']
  print("매도호가: ", price, "매도잔량: ", quant)

print(price)

'''
여러 가상화폐에 대한 정보 한번에 얻기
'''
all = pybithumb.get_current_price("ALL") 
for ticker, data in all.items():
  print(ticker, data)