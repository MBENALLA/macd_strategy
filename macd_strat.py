import pandas as pd
import numpy as np
import talib
from binance.client import Client
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

TIMEFRAME = Client.KLINE_INTERVAL_5MINUTE
SLOW_PERIOD = 26
FAST_PERIOD = 12
MACD_SIGNAL = 9
SPOT_TRADE_FEES = 0.00075

client = Client()

def get_data(coin, look_back):
    df = pd.DataFrame(client.get_historical_klines(coin,
                                                   Client.KLINE_INTERVAL_5MINUTE,
                                                   str(look_back) + ' days ago UTC',
                                                   '5 min ago UTC'))

    closes = pd.DataFrame(df[4])
    closes.columns = ['Close']
    closes.Close = closes.Close.astype(float)
    return closes

engine = create_engine('sqlite:///backtestMACD_db.db')
engine.execute("DELETE FROM COIN_TABLE")
get_data('FTMBUSD', 1).to_sql('COIN_TABLE', if_exists='append', con=engine, index=False)
data = pd.read_sql('COIN_TABLE', con=engine)

def macd_strat(closes, in_position=False):
    closes = closes.copy()
    np_closes = np.asarray(closes.Close)
    macd, macd_signal, macd_hist = talib.MACD(np_closes, fastperiod=FAST_PERIOD, slowperiod=SLOW_PERIOD, signalperiod=MACD_SIGNAL)
    closes['MACD'] = macd
    closes['MACD_SIG'] = macd_signal
    closes.dropna(inplace=True)
    positions = []
    for index, row in closes.iterrows():
        if row['MACD'] > row['MACD_SIG'] and not in_position:
            positions.append(1)
            in_position=True
        elif row['MACD'] < row['MACD_SIG'] and in_position:
            positions.append(1)
            in_position=False
        else:
            positions.append(0)
    closes['Position'] = positions
    closes['v_Position'] = closes['Position'] * closes.Close
    closes = closes[closes['Position'] != 0]
    return closes

def back_test(dataframe):
    dataframe['ROC'] = dataframe.v_Position.pct_change()
    np_roc = np.asarray(dataframe['ROC'])
    roc_size = len(np_roc)
    perf = 0
    if (roc_size % 2) == 0:
        for i in range(0, (roc_size - 1), 2):
            print(np_roc[i + 1])
            perf = perf + np_roc[i + 1]
    else:
        for i in range(0, (roc_size - 2), 2):
            perf = perf + np_roc[i + 1]
    trade_fees = roc_size * SPOT_TRADE_FEES
    return roc_size, trade_fees, perf, dataframe

df1 = macd_strat(data)
trades, fees, performance, frame = back_test(df1)
print(frame)
net_profit = round((performance-fees)*100, 4)
print('Trades: {} | Fees: {}'.format(trades, fees))
print('Performance: {}% | Net profit: {}%'.format(round(performance*100, 4), net_profit))