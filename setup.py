# -*- coding: utf-8 -*-
"""
Created on Tue Jun  2 08:21:37 2020

@author: zheji
"""

from DataManager import minuteDataManager, dailyDataManager, optionDataManager

if __name__ == "__main__":
    debug = True
    x = dailyDataManager('init', debug)
    y = minuteDataManager('init', debug) 
    z = optionDataManager('init', debug)
    x.run()
    y.run()
    z.run()