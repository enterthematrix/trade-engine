"""
IB API - Getting Historical Data iteratively

@author: Sanju (https://github.com/enterthematrix)
"""
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
import pandas as pd
import threading
import time


class TradeApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.data = {}

    def historicalData(self, reqId, bar):
        if reqId not in self.data:
            self.data[reqId] = pd.DataFrame([{"Date": bar.date, "Open": bar.open, "High": bar.high, "Low": bar.low,
                                              "Close": bar.close, "Volume": bar.volume}])
        else:
            self.data[reqId] = pd.concat((self.data[reqId], pd.DataFrame([{"Date": bar.date, "Open": bar.open,
                                                                           "High": bar.high, "Low": bar.low,
                                                                           "Close": bar.close, "Volume": bar.volume}])))
            # self.data[reqId].append({"Date":bar.date,"Open":bar.open,"High":bar.high,"Low":bar.low,"Close":bar.close,"Volume":bar.volume})
        print("reqID:{}, date:{}, open:{}, high:{}, low:{}, close:{}, volume:{}".format(reqId, bar.date, bar.open,
                                                                                        bar.high, bar.low, bar.close,
                                                                                        bar.volume))


def usTechStk(symbol, sec_type="STK", currency="USD", exchange="ISLAND"):
    contract = Contract()
    contract.symbol = symbol
    contract.secType = sec_type
    contract.currency = currency
    contract.exchange = exchange
    return contract


def histData(req_num, contract, duration, candle_size):
    """extracts historical data"""
    app.reqHistoricalData(reqId=req_num,
                          contract=contract,
                          endDateTime='',
                          durationStr=duration,
                          barSizeSetting=candle_size,
                          whatToShow='ADJUSTED_LAST',
                          useRTH=1,
                          formatDate=1,
                          keepUpToDate=0,
                          chartOptions=[])  # EClient function to request contract details

###################storing trade app object in dataframe#######################
def dataDataframe(symbols, TradeApp_obj):
    "returns extracted historical data in dataframe format"
    df_data = {}
    for symbol in symbols:
        df_data[symbol] = pd.DataFrame(TradeApp_obj.data[symbols.index(symbol)])
        df_data[symbol].set_index("Date", inplace=True)
    return df_data

def websocket_con():
    app.run()
    event.wait()
    if event.is_set():
        app.disconnect()


event = threading.Event()
app = TradeApp()
app.connect(host='127.0.0.1', port=7496,
            clientId=23)  # port 4002 for ib gateway paper trading/7497 for TWS paper trading
con_thread = threading.Thread(target=websocket_con)
con_thread.start()
time.sleep(1)  # some latency added to ensure that the connection is established

tickers = ["META", "AMZN", "INTC"]
starttime = time.time()
timeout = time.time() + 60*2
historical_data = {}
while time.time() <= timeout:
    for ticker in tickers:
        histData(tickers.index(ticker),usTechStk(ticker),'3600 S', '30 secs')
        time.sleep(5)
    historical_data = dataDataframe(tickers,app)
    time.sleep(30 - ((time.time() - starttime) % 30.0))

print(historical_data)
event.set()

