# -*- coding: utf-8 -*-
"""
Created on Tue Jun  2 08:21:37 2020

@author: zheji
"""


from DataManager import minuteDataManager, dailyDataManager, optionDataManager

if __name__ == "__main__":
    debug = False
    x = dailyDataManager('update', debug)
    y = minuteDataManager('update', debug) 
    z = optionDataManager('update', debug)
    x.run()
    y.run()
    z.run()