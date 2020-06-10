# -*- coding: utf-8 -*-
"""
Created on Thu May  7 11:09:03 2020

@author: Thomas Chen
"""
#Version 1.2 YahooDataManager
# ---------------------------------------------------------- IMPORTS AND SETTING
import yfinance as yf
import sqlalchemy as db
import argparse
import pandas as pd
import numpy as np
from datetime import datetime
from datetime import timedelta
import json
import os

path = os.getcwd()
yf.pdr_override()
pd.options.display.float_format = '{:,.2f}'.format
#----------------------------------------------------------- Class Definition
class DataManager():
    def __init__(self, mode):
        self.mode = mode
        self.today = (datetime.now()).strftime('%Y-%m-%d')
        self.end_date = (datetime.now()+ timedelta(days=1)).strftime('%Y-%m-%d')  #last date for download
    
    def initialize_params(self, dbname):
        config = json.load(open(path+"/config.json","r"))
        self.con = db.create_engine(config[dbname])
        self.init_path = config['init']
        self.add_path = config['add']
        
    def check_updated(self, s):
        last_update = pd.read_sql(s, self.con)
        if last_update.shape[0] == 0:
            return 'updated' # potentially can raise error
        last_update = last_update.Date.iloc[-1]
        if last_update == self.today:  # check if stock updated
            print("Already Updated ", s)
            return 'updated'
        return last_update
    
    def get_tickers(self):
        if self.mode == 'init':
            return {'init':pd.read_csv(self.init_path).Ticker.unique(),
                    'update':[]}
        elif self.mode == 'update':
            update_stocks = self.con.table_names()
            with open(self.add_path) as f:
                added_stocks = [line for line in f if line not in update_stocks]
            return {'init':added_stocks,
                    'update':update_stocks}
    


    def write_data(self, data, tablename, mode):
        if self.debug:
            print(data.head(5))
        else:
            data.to_sql(tablename, self.con, if_exists=mode, index=False)

    def check_duplicated_data(self):
         for table in self.con.table_names():
            try:
                df = pd.read_sql(table, self.con)
                clean = df.drop_duplicates(keep="first")
                if len(df) != len(clean):
                    print("Duplicates found for ", table)
                    print(clean)
                    if 'level_0' in clean.columns:
                        del clean['level_0']
                    clean.to_sql(table, self.con, if_exists = 'replace')
            except:
                print("Error Occured Checking Data for ", table)
                
                
class dailyDataManager(DataManager):
    def __init__(self, mode, debug):
        super().__init__(mode)
        self.initialize_params('daily')
        self.limit = 3
        self.debug = debug

    def single_stock_download(self, s, start, mode):
        print("Downloading ", s)
        try:
            data = yf.download(s, start=start, end=self.end_date)
            data = data[data.index > start]  # start is t-1
            data = data.reset_index()
            data['Date'] = data.Date.apply(lambda x: x.strftime('%Y-%m-%d'))
            self.write_data(data, s, mode)
            return True
        except:
            print("Failed to download ", s)
            return False

    def initial_download(self, stocks):
        for s in stocks:
            num_counts = 1
            while(num_counts < self.limit):
                flag = self.single_stock_download(s, '2018-01-01', 'replace')
                if flag:
                    break
                num_counts = num_counts + 1

    def update_data(self, stocks):
        for s in stocks:
            last_update = self.check_updated(s)
            if last_update != 'updated':
                num_counts = 1
                while(num_counts < self.limit):
                    flag = self.single_stock_download(s, last_update, 'append')
                    if flag:
                        break
                    num_counts = num_counts + 1
                
    def run(self):
        tickers = self.get_tickers()
        self.update_data(tickers['update'])  # first update data
        self.initial_download(tickers['init'])  # initialize/add new tickers


class minuteDataManager(DataManager):
    def __init__(self, mode, debug):
        super().__init__(mode)
        self.initialize_params('minute')
        self.limit = 3
        self.debug = debug

    def single_stock_minute(self, s, length, mode):
        print("Downloading ", s)
        try:
            ticker = yf.Ticker(s)
            df = ticker.history(period=length, interval='1m')
            
            df['Datetime'] = df.index
            df['Date'] = df['Datetime'].apply(lambda x: x.strftime('%Y-%m-%d'))
            df['Time'] = df['Datetime'].apply(lambda x: x.strftime("%H:%M:%S"))
            df = df[['Date','Time', 'Open', 'High', 'Low', 'Close', 'Volume']]
            df.index = [i for i in range(df.shape[0])]
            
            self.write_data(df, s, mode)
            return True
        except Exception as e:
            print('-'*50)
            print("Failed to download ", s)
            print("Failed Reasons: ", e)
            print('-'*50)
            return False

    def initial_download(self, stocks):
        for s in stocks:
            num_counts = 1
            while(num_counts < self.limit):
                flag = self.single_stock_minute(s, '5d', 'replace')
                if flag:
                    break
                num_counts = num_counts + 1

    def update_data(self, stocks):
        for s in stocks:
            last_update= self.check_updated(s)
            if last_update != 'updated':
                length = str(min(np.busday_count(last_update, datetime.now().strftime("%Y-%m-%d")), 5))+'d'
                num_counts = 1
                while(num_counts < self.limit):
                    flag = self.single_stock_minute(s, length, 'append')
                    if flag:
                        break
                    num_counts = num_counts + 1
                
    def run(self):
        tickers = self.get_tickers() #get ticker for init/update based on mode
        self.update_data(tickers['update'])  # first update data
        self.initial_download(tickers['init'])  # initialize/add new tickers


class optionDataManager(DataManager):
    def __init__(self, mode, debug):
        super().__init__(mode)
        self.initialize_params('option')
        self.limit = 3
        self.debug = debug

    def single_option_chain(self, s, exp, mode):
        try:
            ticker = yf.Ticker(s)
            chain = ticker.option_chain(exp)
            calls = chain.calls
            puts = chain.puts 
            calls['right'] = 'C'
            puts['right'] = 'P'
            df = pd.concat([calls, puts])
            df['ticker'] = s
            df['term'] = (datetime.fromisoformat(exp) + timedelta(days=1)).strftime("%Y-%m-%d")
            df['Date'] = datetime.now().strftime('%Y-%m-%d')
            df['daysToExp'] = np.busday_count(df['Date'], df['term'])
            df['midPrice'] = (df['bid'] + df['ask'])/2
            df = df[['ticker', 'Date', 'term', 'daysToExp', 'strike', 'right',
                     'openInterest', 'volume', 'midPrice', 'impliedVolatility']]
            self.write_data(df, s, mode)
            return True
        except Exception as e:
            print("Failed to download options for {} with expiration {}".format(s, exp))
            print("Failed Reason: ", e)
            return False
        
    def get_exp(self, s):
        try:
            ticker = yf.Ticker(s)
            exps = ticker.options
            return (True, exps)
        except:
            return (False, [])

    def update_data(self, stocks):
        for s in stocks:
            if self.check_updated(s) == 'updated': #if updated then skip
                continue
            
            success, exps = self.get_exp(s) # getting expirations
            if not success:
                print("Getting expirations unsucessful")
                continue
            
            #successful got exps
            for exp in exps:
                num_counts = 1
                while(num_counts < self.limit):
                    flag = self.single_option_chain(s, exp, 'append')
                    if flag:
                        break
                    num_counts = num_counts + 1
                
    def run(self):
        tickers = self.get_tickers() #get ticker for init/update based on mode
        self.update_data(tickers['update'])  #  update today option data