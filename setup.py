# -*- coding: utf-8 -*-
"""
Created on Tue Jun  2 08:21:37 2020

@author: zheji
"""

from DataManager import minuteDataManager, dailyDataManager, optionDataManager, recDataManager

if __name__ == "__main__":
    debug = False
    x = dailyDataManager('init', debug)
    # y = minuteDataManager('init', debug)
    # z = optionDataManager('init', debug)
    # r = recDataManager('init', debug)
    x.run('2012-01-01')
    # y.run()
    # z.run()
    # r.run()

