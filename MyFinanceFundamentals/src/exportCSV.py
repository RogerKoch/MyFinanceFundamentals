#!/usr/bin/python
# -*- coding: UTF-8 -*-

from getDatabaseConnection import DatabaseManager
import datetime
import csv
import os
from sendEmail import sendEmail


def exportCsv(currrentYear):
    
    if currrentYear is None:
        today = datetime.datetime.now()
        currrentYear = today.year 

    db = DatabaseManager()
    mydb = db.setDBConnection('stockInfo')
    myCursor = mydb.cursor()    
    divCursor = mydb.cursor()
    epsCursor = mydb.cursor()
    #Financials    
    selectKeyFigures = """
                            SELECT bd.Isin, bd.yahooTicker, bd.Name, bd.currency, bd.Sector, bd.Industry, keyFigures.Hight_52, keyFigures.Low_52, keyFigures.PERatio, keyFigures.BookValue, keyFigures.PriceToBook, keyFigures.LastDividend, keyFigures.LastDividendYield, keyFigures.LastDividendDate, timeseries.TSValue , keyFigures.KeyDate, timeseries.TSDate, indexInfo.indexName FROM stockInfo.BasicData AS bd
                            INNER JOIN (
                            
                                        SELECT kf.isin, kf.Hight_52, kf.Low_52, kf.PERatio , kf.BookValue, kf.PriceToBook, kf.LastDividend, kf.LastDividendYield, kf.LastDividendDate, kf.KeyDate FROM stockInfo.KeyFigures AS kf
                                        INNER JOIN (SELECT isin, max(KeyDate) as kfDate FROM stockInfo.KeyFigures
                                                    GROUP BY isin
                                                    ) maxKF
                                        ON maxKF.isin=kf.isin AND maxKF.kfDate=kf.keyDate
                                        ) keyFigures
                            ON keyFigures.isin = bd.isin            
                            INNER JOIN (SELECT ts.isin, ts.TSDate,  ts.TSValue FROM stockInfo.Timeseries AS ts
                                        INNER JOIN (SELECT isin, max(TSDate) as tsDate FROM stockInfo.Timeseries
                                                    GROUP BY isin
                                                    ) maxTS
                                        on maxTS.isin = ts.isin AND maxTS.tsDate=ts.tsDate
                                        ) timeseries
                            ON timeseries.isin=bd.isin
                            INNER JOIN (SELECT bd.isin, group_concat(inf.indexname SEPARATOR ', ') AS indexName FROM stockInfo.BasicData AS bd
                                        INNER JOIN stockInfo.IndexDetails AS ind
                                        ON ind.isin=bd.isin
                                        INNER JOIN stockInfo.IndexInformation AS inf
                                        ON inf.idIndexInformation=ind.indexId
                                        GROUP BY bd.isin
                                        ) indexInfo
                            ON indexInfo.isin = bd.isin
                            """
    selectDividends = """
                        SELECT DIVYear, DIVValue FROM stockInfo.DivFigures
                        WHERE ISIN='{0}'
                    """
                    
    selectEPS = """
                    SELECT EPSYear, EPSValue FROM stockInfo.EPSFigures
                    WHERE ISIN='{0}'
                    ORDER BY EPSYear ASC
                """
    
    yearRange = range(currrentYear - 4, currrentYear + 1)
    
    keyTitleRow = ['ISIN', 'YahooTicker', 'Name', 'Currency', 'Sector', 'Industry', 'Hight_52', 'Low_52', 'PeRatio', 'BookValue', 'PriceToBook', 'LastDividend', 'LastDividendYield', 'LastDividendDate', 'TSValue' , 'KeyDate', 'TSDate', 'IndexName§  0        ']
    divTitleRow = ['FirstDivYear', 'LastDivYear', 'DivIncreas', 'DivDecrease', 'DivGrowthSi', 'DivGrowthLast7', 'DivGrowthLast5', 'DivGrowthlast3', 'DivGrowthLast']
    divYearTitleRow = []
    payoutYearTitleRow = []
    for year in yearRange:
        divYearTitleRow.append('DivYear_{0}'.format(year))
        payoutYearTitleRow.append('PayoutYear_{0}'.format(year))
    
    curPath = os.path.dirname(os.path.realpath(__file__))
    csvPath = os.path.join(curPath, 'export\\financials.csv')
    titleRow = keyTitleRow + divTitleRow + divYearTitleRow + payoutYearTitleRow
    with open(csvPath, mode='w', newline='') as fin_file:
        fin_writer = csv.writer(fin_file, delimiter=';', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        fin_writer.writerow(titleRow)    
        myCursor.execute(selectKeyFigures)
        allFinancials = myCursor.fetchall()
        for sqlRow in allFinancials:
            isin = sqlRow[0]
            divCursor.execute(selectDividends.format(isin))
            divRow, divYearDict = calcDividends(divCursor.fetchall(), yearRange)
            epsCursor.execute(selectEPS.format(isin))
            payoutRow, divYearRow = calcPayout(epsCursor.fetchall(), divYearDict, yearRange)
            
            dataRow = list(sqlRow) + divRow + divYearRow + payoutRow
            
            fin_writer.writerow(dataRow)    
    
    
#     #Dividends    
#     selectDiv = """
#                     SELECT bd.isin, di.DIVFiguresType, di.DIVFiguresYear, di.DIVFiguresValue FROM stockInfo.DivFigures AS di
#                     RIGHT JOIN stockInfo.BasicData AS bd
#                     ON di.ISIN=bd.isin
#                 """
#     titleRow = ['ISIN','DIVFiguresType', 'DIVFiguresYear', 'DIVFiguresValue']
#     with open('export/dividends.csv', mode='w', newline='') as div_file:
#         div_writer = csv.writer(div_file, delimiter=';', quotechar='"', quoting=csv.QUOTE_MINIMAL)
#         div_writer.writerow(titleRow)    
#         myCursor.execute(selectDiv)
#         allDividends = myCursor.fetchall()
#         for sqlRow in allDividends:
#             div_writer.writerow(sqlRow)        
    
    myCursor.close()
    divCursor.close()
    epsCursor.close()
    
    sendEmail()


def calcDividends(divTable, yearRange):
    if len(divTable) > 0:
        firstYear = divTable[0][0]
        lastYear = divTable[-1][0]
        
        firstYearValue = divTable[0][1]
        lastYearValue = divTable[-1][1]
        
        yearM7 = lastYear - 7
        yearM5 = lastYear - 5
        yearM3 = lastYear - 3
        yearM1 = lastYear - 1
        
        divDict = {}
        for row in divTable:
            divDict[row[0]] = row[1]
        
        yearM1Value = divDict.get(yearM1, None)
        yearM3Value = divDict.get(yearM3, None)
        yearM5Value = divDict.get(yearM5, None)
        yearM7Value = divDict.get(yearM7, None)
        
        
        if firstYearValue == 0:
            siPerf = None
        else:
            siPerf = (lastYearValue - firstYearValue) / firstYearValue
        if yearM7Value is None:
            perfM7 = None
        else:
            perfM7 = (lastYearValue - yearM7Value) / yearM7Value
        if yearM5Value is None:
            perfM5 = None
        else:
            perfM5 = (lastYearValue - yearM5Value) / yearM5Value
        if yearM3Value is None:
            perfM3 = None
        else:    
            perfM3 = (lastYearValue - yearM3Value) / yearM3Value
        if yearM1Value is None:
            perfM1 = None
        else:    
            perfM1 = (lastYearValue - yearM1Value) / yearM1Value
        
        countInc = countDec = 0
        calcDict = {}
        first = True
        for i in range(len(divTable)):
            if first is True:
                first = False
            else:
                if divTable[i][1] > divTable[i-1][1]:
                    countInc = countInc + 1
                elif divTable[i][1] < divTable[i-1][1]:
                    countDec = countDec + 1
                
                if divTable[i][0] in yearRange:
                    calcDict[divTable[i][0]] = divTable[i][1]
                    
        divRow = [firstYear, lastYear, countInc, countDec, siPerf, perfM7, perfM5, perfM3, perfM1]
    else:
        divRow = 9 * [None]
        calcDict = {}

    return divRow, calcDict

def calcPayout(epsTable, divYearDict, yearRange):
    epsDict = {}
    for row in epsTable:
        epsDict[row[0]] = row[1]
    
    payoutRow = []
    divYearRow=[]
    for year in yearRange:
        divValue = divYearDict.get(year, None)
        epsValue = epsDict.get(year, None)
        if divValue is not None and epsValue is not None:
            if epsValue==0:
                payout = None
            else:
                payout = divValue / epsValue
        else:
            payout = None
        payoutRow.append(payout)
        divYearRow.append(divValue)

    return payoutRow, divYearRow

if __name__ == '__main__':
    exportCsv(None)
