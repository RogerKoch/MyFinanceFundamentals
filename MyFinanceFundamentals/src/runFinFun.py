#!/usr/bin/python
# -*- coding: UTF-8 -*-

from updateIndexInformation import updateIndexInformation
from updateBasicData import updateBasicData
from updateTimeseries import updateTimeseries
from updateKeyFigures import updateKeyFigures
from updateDividend import updateDividend
from updateDilutedEPS import updateDilutedEPS
from exportCSV import exportCsv

if __name__ == '__main__':
#     #Update the Index information based on the table values from database.IndexInforamtion
#     #For a new Index please fill in in the database.IndexInforamtion a new Index
#     #    -> IndexExchange => index shortcute in openfigi
#     updateIndexInformation()
#     print('FINISH:\t updateIndexInformation')
#     
#     #All stocks are inserted or updated based on the index
#     #Update the stock basic information with yahoo finance data
#     updateBasicData()
#     print('FINISH:\t updateBasicData')    
#     
#     #Update/Insert the timeseries data of the stock
#     #from the max date found until the date you set in the function
#     updateTimeseries(None, None, False)
#     print('FINISH:\t updateTimeseries')    
#      
#     #Update Keyfigures based on the run date from yahoo one a month
#     updateKeyFigures()
#     print('FINISH:\t updateKeyFigures')
#      
#     #Update dividend history from yahooFinance one a month
#     updateDividend()
#     print('FINISH:\t updateDividend')    
#      
#     #Update EPS history from yahooFinance one a month
#     updateDilutedEPS()
#     print('FINISH:\t updateDilutedEPS')
#      
#     #Export the current data to csv files into the export Folder
    exportCsv(None)
