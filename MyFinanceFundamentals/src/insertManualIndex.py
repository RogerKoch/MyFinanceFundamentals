#!/usr/bin/python
# -*- coding: UTF-8 -*-

from getDatabaseConnection import DatabaseManager
from Isin2Ticker import getMappingResult
import xlrd


def chunks(l, n):
    # For item i in a range that is a length of l,
    for i in range(0, len(l), n):
        # Create an index range for l of n items:
        yield l[i:i+n]


def insertManualIndex(fileName): 

    db = DatabaseManager()
    mydb = db.setDBConnection('stockInfo')
    errorCursor = mydb.cursor()
    errorQuery = """Call stockInfo.spInsError ('{0}', '{1}', '{2}')"""    

    try:            
        xlFile = xlrd.open_workbook('import/{0}.xlsx'.format(fileName))
        xlSheet = xlFile.sheet_by_index(0)
        

        
        indexName = xlSheet.cell_value(0, 1)
        myCursor = mydb.cursor()
        myCursor.execute("""CALL stockInfo.spInsManualIndex ('{0}')""".format(indexName))
        mydb.commit()
        selectCursor = mydb.cursor()
        selectQuery = "SELECT idIndexInformation FROM stockInfo.IndexInformation WHERE IndexName='{0}';".format(indexName)
        selectCursor.execute(selectQuery)
        indexIDList = selectCursor.fetchall()
        idIndexInformation = indexIDList[0][0]
        
        exchangeGroupDict = {}
        instrumentInfos = {}
        for row_idx in range(3, 500):
            if row_idx >=xlSheet.nrows:
                break
            isin = str(xlSheet.cell_value(row_idx, 0))
            if len(isin) == 0:
                break
            stockName = str(xlSheet.cell_value(row_idx, 1))
            indexExchange = str(xlSheet.cell_value(row_idx, 2))
            yahooTicker = str(xlSheet.cell_value(row_idx, 3))
            
            if indexExchange == "ERROR" and len(yahooTicker) == 0:
                errorText = "Can't import ISIN: {0}".format(isin)
                print(errorText)
            elif len(yahooTicker) > 0:
                instrumentInfos[isin]=[isin, stockName, yahooTicker]
            else:
                instrumentInfos[isin]=[isin, stockName]
                if indexExchange in exchangeGroupDict:
                    exchangeGroupDict[indexExchange].append(isin)
                else:
                    exchangeGroupDict[indexExchange] = [isin]
        
        
        for exchangeKey in exchangeGroupDict:
            yahooSelect = """SELECT YahooExchange FROM stockInfo.stockExchangeTranslation WHERE StockExchange='{0}'""" .format(exchangeKey)
            myCursor = mydb.cursor()
            myCursor.execute(yahooSelect)
            yahooExchange = myCursor.fetchall()[0][0]            
            
            list_of_groups = list(chunks(list(exchangeGroupDict.get(exchangeKey)), 90))
            for group in list_of_groups:
                mappingRes = getMappingResult(group, exchangeKey)
                for key in mappingRes.keys():
                    isinList = mappingRes.get(key)
                    if yahooExchange is not None and len(yahooExchange) > 0:
                        instrumentInfos.get(key).append(isinList[0] +"."+ yahooExchange)
                    else:
                        instrumentInfos.get(key).append(isinList[0])                     
        
        
        insertQuery = ("""Call stockInfo.spInsBasicDataManual (%s, %s, %s)""")
        insertIndexQuery = ("""Call stockInfo.spInsIndexDetail (%s, %s)""")
        insertCursor = mydb.cursor()
        for line in instrumentInfos:            
            insertLine = instrumentInfos.get(line)
            if len(insertLine) > 2:
                insertData = (insertLine[0].replace("'", "''"), insertLine[1].replace("'", "''"), insertLine[2])
                insertIndexData = (idIndexInformation, insertLine[0].replace("'", "''"))
                insertCursor.execute(insertQuery, insertData)
                insertCursor.execute(insertIndexQuery, insertIndexData)
            else:
                errorCursor.execute(errorQuery.format(insertLine[0].replace("'", "''"), 'insertManualIndex', 'No YahooTicker'))
        mydb.commit()
        insertCursor.close()

    except Exception as e:
        errorCursor.execute(errorQuery.format('N/A', 'insertManualIndex', str(e).replace("'", "''")))
    
    mydb.commit()    
    errorCursor.close()
      

if __name__ == '__main__':
    insertManualIndex('USDividendAristokrates')



