#!/usr/bin/python
# -*- coding: UTF-8 -*-

from getDatabaseConnection import DatabaseManager
from bs4 import BeautifulSoup
from Isin2Ticker import getMappingResult
import time
import requests



def chunks(l, n):
    # For item i in a range that is a length of l,
    for i in range(0, len(l), n):
        # Create an index range for l of n items:
        yield l[i:i+n]

def pageLinks(url):

    html_content = requests.get(url).text

    bs = BeautifulSoup(html_content, 'lxml')
    listContainer = bs.find('div', attrs={'id': 'index-list-container'})

    allPages = listContainer.find('div', attrs={'class': 'finando_paging'})
    pagesLinks = allPages.findAll('a')
    pagesList = []
    if len(pagesLinks) == 0:
        pagesList.append('')
    else:
        for a in pagesLinks:
            pagesList.append(a['href'])

    return pagesList


def updateIndexInformation(): 
    
    try:
        db = DatabaseManager()
        mydb = db.setDBConnection('stockInfo')
        
        errorCursor = mydb.cursor()
        errorQuery = """Call stockInfo.spInsError ('{0}', '{1}', '{2}')"""        
        
        selectCursor = mydb.cursor(buffered=True)
        
        #selectQuery = "SELECT idIndexInformation, IndexURL, IndexExchange FROM stockInfo.IndexInformation WHERE isMixed=0;"
        selectQuery = "SELECT idIndexInformation, IndexURL, IndexExchange FROM stockInfo.IndexInformation WHERE isMixed=0 AND idIndexInformation = 4"
        selectCursor.execute(selectQuery)
        
        for (idIndexInformation, url, indexExchange) in selectCursor:
        
            pageList = pageLinks(url)
            instrumentInfos = {}
            
            yahooSelect = """SELECT YahooExchange FROM stockInfo.stockExchangeTranslation WHERE StockExchange='{0}'""" .format(indexExchange)
            myCursor = mydb.cursor()
            myCursor.execute(yahooSelect)
            yahooExchange = myCursor.fetchall()[0][0]
            
            for page in pageList:
                pageUrl = url + page# pageList[0]
                page_content = requests.get(pageUrl).text
                bsPage = BeautifulSoup(page_content, 'lxml')
                listContainer = bsPage.find('div', attrs={'id': 'index-list-container'})
                indexTable = listContainer.find('table')
                rows = indexTable.find_all("tr")[1:]
            
                for row in rows:
                    row_data = row.find_all("td")
                    cellText = row_data[0].text
                    name, isin = cellText.split('\r\n')
                    cellHref = row_data[0].find('a')['href']
                    bilanzKey = cellHref.split('/')[-1][:-6]
                    instrumentInfos[isin.strip()] = [isin.strip(), name.strip(), bilanzKey.strip()]
            
            if len(indexExchange) > 0:
                list_of_groups = list(chunks(list(instrumentInfos.keys()), 90))
                for group in list_of_groups:
                    mappingRes = getMappingResult(group, indexExchange)
                    for key in mappingRes.keys():
                        isinList = mappingRes.get(key)
                        if yahooExchange is not None and len(yahooExchange) > 0:
                            instrumentInfos.get(key).append(isinList[0] +"."+ yahooExchange)
                        else:
                            instrumentInfos.get(key).append(isinList[0])
                        instrumentInfos.get(key).append(isinList[2])
                        instrumentInfos.get(key).append(isinList[3])
            else:
                for isin in instrumentInfos.keys():
                    instrumentInfos.get(isin).append('')
                    instrumentInfos.get(isin).append('')
                    instrumentInfos.get(isin).append('')
            
            insertQuery = ("""Call stockInfo.spInsBasicData (%s, %s, %s, %s)""")
            insertIndexQuery = ("""Call stockInfo.spInsIndexDetail (%s, %s)""")
            insertCursor = mydb.cursor()
            for line in instrumentInfos:
                insertLine = instrumentInfos.get(line)
                insertData = (insertLine[0].replace("'", "''"),insertLine[1].replace("'", "''"),insertLine[3],insertLine[2])
                insertIndexData = (idIndexInformation, insertLine[0].replace("'", "''"))
                insertCursor.execute(insertQuery, insertData)
                insertCursor.execute(insertIndexQuery, insertIndexData)
            mydb.commit()
            insertCursor.close()
            time.sleep(10)
        
        selectCursor.close()
    except Exception as e:
        errorCursor.execute(errorQuery.format('N/A', 'updateIndexInformation', str(e).replace("'", "''")))
    
    
    errorCursor.execute(errorQuery.format('FINISH', 'updateIndexInformation', 'FINISH IMPORT'))    
    errorCursor.close()
    mydb.commit()
    
    

if __name__ == '__main__':
    updateIndexInformation()



