import csv
import datetime
import math
import time
import logging
import os.path
from abc import abstractmethod
from asyncio import wait
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pandas as pd
import tqdm
from binance.spot import Spot
from binance.um_futures import UMFutures

from retry import retry

logging.basicConfig(format="%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s",
                    level=logging.INFO)

max_workers = 5


class BaseConfig:
    header = ["T", "O", "H", "L", "C", "V", "A", "N", "BV", "BA"]
    """
        T: time k线开盘时间
        O: open price  开盘价
        H: high price  最高价
        L: low price   最低价
        C: close price 收盘价
        V:  成交量
        A:  成交额
        N:  成交笔数
        BV： 主动买入成交量
        BA:  主动买入成交额
        
        1499040000000,      // k线开盘时间
        "0.01634790",       // 开盘价
        "0.80000000",       // 最高价
        "0.01575800",       // 最低价
        "0.01577100",       // 收盘价(当前K线未结束的即为最新价)
        "148976.11427815",  // 成交量
        1499644799999,      // k线收盘时间
        "2434.19055334",    // 成交额
        308,                // 成交笔数
        "1756.87402397",    // 主动买入成交量
        "28.46694368",      // 主动买入成交额
        "17928899.62484339" // 请忽略该参数
    """

    def __init__(self, save_dir, symbol, timestamp=60 * 1000):
        self.save_dir = save_dir
        self.symbol = symbol
        self.file_name = f"{symbol}.csv"
        self.file_path = os.path.join(self.save_dir, self.file_name)
        self.timestamp = timestamp
        self.start_time = 0
        self.chunk_size = 10 ** 7
        self.check()

    def check(self):
        if not os.path.exists(self.save_dir):
            os.makedirs(self.save_dir)
        if os.path.exists(self.file_path):
            with open(self.file_path, "r", encoding="utf-8", errors="ignore") as scraped:
                final_line = scraped.readlines()[-2:]
                final_line = [item for item in final_line if len(item) > 2]
                if len(final_line) > 1:
                    v = final_line[-1].split(",")
                    self.start_time = int(v[0]) + self.timestamp
        else:
            with open(self.file_path, "w", encoding='UTF8', newline='') as file:
                writer = csv.writer(file)
                # write the header
                writer.writerow(self.header)

    def run(self, datas):
        if datas is not None and len(datas) > 0:
            with open(self.file_path, "a", encoding='UTF8', newline='') as file:
                writer = csv.writer(file)
                writer.writerows(datas)
                self.start_time = int(datas[-1][0]) + self.timestamp

    def scan_process(self, chunk):
        start_time = chunk["T"].iloc[0]
        end_time = chunk["T"].iloc[-1]
        total = chunk["T"].size
        end_time_stamp = start_time + (total - 1) * self.timestamp
        logging.info(f"missing interval {self.symbol} {(end_time - end_time_stamp) // self.timestamp}")
        return math.fabs(end_time - end_time_stamp) < 1e-3

    def check_missing(self):
        start = time.time()
        miss = False
        with pd.read_csv(self.file_path, chunksize=self.chunk_size) as reader:
            for chunk in reader:
                miss_value = self.scan_process(chunk)
                miss = miss if miss else miss_value
        end = time.time()
        logging.info(f"scanned finished {self.symbol} {end - start}s")
        return miss

    @abstractmethod
    def request_range(self, start_t, end_t):
        raise NotImplementedError

    def chunk_scan(self, chunk_data):
        if not self.scan_process(chunk_data):
            total = chunk_data["T"].size
            start_time = int(chunk_data["T"].iloc[0])
            for i in tqdm.tqdm(range(total), position=0, desc=f"scan row {self.symbol}"):
                if chunk_data["T"].iloc[i] - start_time > 1e-5:
                    end_time = chunk_data["T"].iloc[i] - 1
                    add_chunk_data = self.request_range(start_time, end_time)
                    if len(add_chunk_data) > 0:
                        iter_idx = 1 / (len(add_chunk_data) + 10)
                        for item in add_chunk_data:
                            chunk_data.loc[i + iter_idx] = pd.DataFrame([item], columns=self.header).loc[0]
                            iter_idx = iter_idx + 1 / (len(add_chunk_data) + 10)
                    start_time = int(chunk_data["T"].iloc[i])
                start_time = start_time + self.timestamp
            chunk_data = chunk_data.sort_index().reset_index(drop=True)
            return chunk_data

    def chunk_modify(self):
        copy_file_path = self.file_path + "_temp.csv"
        if os.path.exists(copy_file_path):
            os.remove(copy_file_path)
        header = True
        with pd.read_csv(self.file_path, chunksize=self.chunk_size) as reader:
            for chunk in reader:
                chunk = self.chunk_scan(chunk)
                chunk.to_csv(copy_file_path, index=False, header=header, columns=self.header, mode="a")
                header = False
        os.remove(self.file_path)
        os.rename(copy_file_path, self.file_path)

    def scan(self):
        if not self.check_missing():
            logging.error(f"check failed  {self.symbol}")
            # self.chunk_modify()
            pass


class CrawCoin(BaseConfig):
    _idx = [0, 1, 2, 3, 4, 5, 7, 8, 9, 10]

    def __init__(self, client, interval, symbol, time_interval, save_dir=os.path.abspath(__file__), **kwargs):
        super().__init__(symbol=symbol, save_dir=save_dir, timestamp=time_interval, **kwargs)
        self.interval = interval
        self.offset = 1000
        self.symbol = symbol
        self.client = client

    def get_start_time(self):
        start = self.start_time
        if self.start_time == 0:
            # 初始化获取symbol start_time
            k_datas = self.client.klines(self.symbol, self.interval, startTime=0, limit=1)
            start = k_datas[0][0]
        return start

    @retry(Exception, tries=5, delay=1)
    def request_data(self):
        k_datas = self.client.klines(self.symbol, self.interval, startTime=self.start_time, limit=self.offset)
        return k_datas

    @retry(Exception, tries=5, delay=1)
    def request_range(self, start_t, end_t):
        k_datas = self.client.klines(self.symbol, self.interval, startTime=start_t, endTime=end_t)
        return self.process(k_datas)

    def process(self, datas):
        if datas is not None and len(datas) > 0:
            return [np.array(item)[self._idx] for item in datas]
        return datas

    def __call__(self, *args, **kwargs):
        # logging.info(f"start running client time {str(self.client.time())} symbol: {self.symbol}")
        start = self.get_start_time()
        current_time = int(1000 * datetime.datetime.now().timestamp())
        total_range = (current_time - start) // (self.offset * self.timestamp)
        for _ in tqdm.tqdm(range(total_range), desc=f"process: {self.symbol} ", position=0):
            # pbar.set_description(f"Processing {self.symbol}")
            datas = self.request_data()
            datas = self.process(datas)
            self.run(datas)


class CrawlSportCoin(CrawCoin):
    def __init__(self, symbol, **kwargs):
        super().__init__(client=Spot(), symbol=symbol, **kwargs)


class CrawFutureCoin(CrawCoin):
    def __init__(self, symbol, **kwargs):
        client = UMFutures()
        super().__init__(client=client, symbol=symbol, **kwargs)


class CrawAllCoin:
    def __init__(self, save_dir, client):
        self.save_dir = save_dir
        self.interval = "1m"
        self.time_interval = 60 * 1000
        result = client.exchange_info()
        self.symbols = [item['symbol'] for item in result["symbols"]]
        self.symbols = self.symbols[:15]
        print(self.symbols)

    @abstractmethod
    def sync_symbol(self, symbol):
        raise NotImplementedError

    def run(self):
        # create a thread pool
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(self.sync_symbol, symbol) for symbol in self.symbols]
            # for symbol in self.symbols:
            #     self.sync_symbol(symbol)
            wait(futures)
        logging.info("finished sync all coin data")


class CrawAllSportCoin(CrawAllCoin):
    def __init__(self, save_dir):
        super().__init__(client=Spot(), save_dir=save_dir)

    @retry(Exception, tries=5, delay=1)
    def sync_symbol(self, symbol):
        craw = CrawlSportCoin(save_dir=self.save_dir, symbol=symbol, interval=self.interval,
                              time_interval=self.time_interval)
        craw()
        craw.scan()


class CrawAllFutureCoin(CrawAllCoin):
    def __init__(self, save_dir):
        super().__init__(client=UMFutures(), save_dir=save_dir)

    @retry(Exception, tries=5, delay=1)
    def sync_symbol(self, symbol):
        craw = CrawFutureCoin(save_dir=self.save_dir, symbol=symbol, interval=self.interval,
                              time_interval=self.time_interval)
        craw()
        craw.scan()


# craw = CrawFutureCoin(save_dir="./future", symbol="BTCUSDT", interval="1m", time_interval=60 * 1000)
# craw.scan()

# a = CrawAllSportCoin(save_dir="./sport")
# a.run()

a = CrawAllFutureCoin(save_dir="./future")
# a.run()

# Get server timestamp
# interval = {
#     "key": "1m",
#     "time": 60 * 1000
# }
# Get klines of BTCUSDT at 1m interval
# craw = CrawFutureCoin(save_dir="./future", symbol="BNBUSDT", interval="1m", time_interval=60 * 1000)
# craw.scan()
# craw()
