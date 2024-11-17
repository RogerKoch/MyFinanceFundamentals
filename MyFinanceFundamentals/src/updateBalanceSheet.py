#!/usr/bin/python
# -*- coding: UTF-8 -*-

from bs4 import BeautifulSoup
from selenium import webdriver

from getDatabaseConnection import DatabaseManager
import time

from projectConstant import BALANCESHEET_KEYWORDS


def updateBalanceSheet():
    
    db = DatabaseManager()
    mydb = db.setDBConnection('stockInfo')
    
    errorCursor = mydb.cursor()
    errorQuery = """Call stockInfo.spInsError ('{0}', '{1}', '{2}')"""  

    try:
    
        myCursor = mydb.cursor()   
    
        baseURL = 'https://finance.yahoo.com/quote/{0}/balance-sheet?p={0}'   
                
        #Get all ISINs    
        insertQuery = ("""Call stockInfo.spInsBalanceSheet (%s, %s, %s, %s)""")
        insertCursor = mydb.cursor()
        
        myCursor.execute('SELECT isin, yahooTicker FROM stockInfo.BasicData LIMIT 10')
        
        allIsins = myCursor.fetchall()
        for sqlRow in allIsins:    
            try:
                isin = sqlRow[0]
                ticker = sqlRow[1]
            
                runURL = baseURL.format(ticker)
    
                driver = webdriver.Chrome('./chromedriver')
                driver.get(runURL)
                button = driver.find_element_by_class_name('expandPf')
                button.click()    
                
                html = driver.page_source.encode('utf-8')
                bs = BeautifulSoup(html, 'lxml')
            
                finHeader =bs.find('span', text='Breakdown').parent.parent
                for searchWord in BALANCESHEET_KEYWORDS:
                    findWord = bs.find('span', text=searchWord)
                    if findWord is not None:
                        balanceRow = findWord.parent.parent.parent
                        valueRows = balanceRow.find_all('span')
                        headerRows = finHeader.find_all('span')
                                                               
                        for i in range(1,len(valueRows)):
                            _m,_d,year = headerRows[-i].text.split('/')
                            balanceValue = float(valueRows[-i].text.replace(',',''))      
                            insertData = (isin, searchWord, year, balanceValue)
                            insertCursor.execute(insertQuery, insertData)
                            mydb.commit()           
                time.sleep(1)
            except KeyError as e:
                errorCursor.execute(errorQuery.format(isin, 'BalanceSheet', str(e).replace("'", "''")))
            except ValueError as e:
                errorCursor.execute(errorQuery.format(isin, 'BalanceSheet', str(e).replace("'", "''")))
            except Exception as e:
                    errorCursor.execute(errorQuery.format(isin, 'BalanceSheet', str(e).replace("'", "''")))
        mydb.commit()  
        insertCursor.close()
        myCursor.close()
    except Exception as e:
        errorCursor.execute(errorQuery.format('N/A', 'BalanceSheet', str(e).replace("'", "''")))
        
    errorCursor.execute(errorQuery.format('FINISH', 'BalanceSheet', 'FINISH IMPORT'))    
    errorCursor.close()
    mydb.commit()                    
        
    


if __name__ == '__main__':
    updateBalanceSheet()

