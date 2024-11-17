#!/usr/bin/python
# -*- coding: UTF-8 -*-

from bs4 import BeautifulSoup
from selenium import webdriver

from getDatabaseConnection import DatabaseManager
import yfinance as yf
import time
import requests

from projectConstant import BALANCESHEET_KEYWORDS


def getFinancials(index, name):
    ticker = yf.Ticker('PM')   
    fin = ticker.financials
    print('##########')
    print(name)
    print('##########')
    ebit = fin.iloc[index]
    column = list(fin.columns)
    for c in column:
        print('{0}, {1}'.format(c, ebit[c]))
        
def getBalance(index, name):
    ticker = yf.Ticker('PM')   
    bal = ticker.balance_sheet
    print('##########')
    print(name)
    print('##########')
    ebit = bal.iloc[index]
    column = list(bal.columns)
    for c in column:
        print('{0}, {1}'.format(c, ebit[c]))
    
    

def manualFinancials(searchWord):
    baseURL = 'https://finance.yahoo.com/quote/{0}/financials?p={0}'    
        
    ticker = 'PM'
    print('##########')
    print(searchWord)
    print('##########')
    runURL = baseURL.format(ticker)
       
    page_content = requests.get(runURL).text
    bs = BeautifulSoup(page_content, 'lxml')
    finHeader =bs.find('span', text='Breakdown').parent.parent
            
    finRow = bs.find('span', text=searchWord).parent.parent.parent
    valueRows = finRow.find_all('span')
    headerRows = finHeader.find_all('span')
                                           
    for i in range(1,len(valueRows)):
        colText = headerRows[-i].text
        if colText.find('/') > -1:
            _m,_d,year = headerRows[-i].text.split('/')
            value = float(valueRows[-i].text.replace(',',''))       
            print('{0}, {1}'.format(year, value))
        else:
            break
    time.sleep(1)
    
def manualBalance(searchWord):
    baseURL = 'https://finance.yahoo.com/quote/{0}/balance-sheet?p={0}'    
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'}
        
    ticker = 'PM'
    print('##########')
    print(searchWord)
    print('##########')
    runURL = baseURL.format(ticker)
       
    page_content = requests.get(runURL, headers=headers).text
    bs = BeautifulSoup(page_content, 'lxml')
    finHeader =bs.find('span', text='Breakdown').parent.parent
            
    finRow = bs.find('span', text=searchWord).parent.parent.parent
    valueRows = finRow.find_all('span')
    headerRows = finHeader.find_all('span')
                                           
    for i in range(1,len(valueRows)):
        colText = headerRows[-i].text
        if colText.find('/') > -1:
            _m,_d,year = headerRows[-i].text.split('/')
            value = float(valueRows[-i].text.replace(',',''))       
            print('{0}, {1}'.format(year, value))
        else:
            break
    time.sleep(1)
    
    
def spezManualBalance(searchWord):
    baseURL = 'https://finance.yahoo.com/quote/{0}/balance-sheet?p={0}'    
        
    ticker = 'PM'
    print('##########')
    print(searchWord)
    print('##########')
    runURL = baseURL.format(ticker)
    
    driver = webdriver.Chrome('./chromedriver')
    driver.get(runURL)
    button = driver.find_elements_by_css_selector("[aria-label='Total Equity Gross Minority Interest']")
    button[0].click()

    html = driver.page_source.encode('utf-8')
    bs = BeautifulSoup(html, 'lxml')

    finHeader =bs.find('span', text='Breakdown').parent.parent
            
    finRow = bs.find('span', text=searchWord).parent.parent.parent
    valueRows = finRow.find_all('span')
    headerRows = finHeader.find_all('span')
                                           
    for i in range(1,len(valueRows)):
        colText = headerRows[-i].text
        if colText.find('/') > -1:
            _m,_d,year = headerRows[-i].text.split('/')
            value = float(valueRows[-i].text.replace(',',''))       
            print('{0}, {1}'.format(year, value))
        else:
            break
    time.sleep(1)        
        
    
def totalBalance():
    baseURL = 'https://finance.yahoo.com/quote/{0}/balance-sheet?p={0}'    
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'}
        
    ticker = 'PM'
    runURL = baseURL.format(ticker)
    
    driver = webdriver.Chrome('./chromedriver')
    driver.get(runURL)
    #button = driver.find_elements_by_css_selector("""[class="{0}"]""".format('expandPf'))
    button = driver.find_element_by_class_name('expandPf')
    button.click()    
    
    html = driver.page_source.encode('utf-8')
    bs = BeautifulSoup(html, 'lxml')

    finHeader =bs.find('span', text='Breakdown').parent.parent
    
    for searchWord in BALANCESHEET_KEYWORDS:
        print('##########')
        print(searchWord)
        print('##########')
        finRow = bs.find('span', text=searchWord).parent.parent.parent
        valueRows = finRow.find_all('span')
        headerRows = finHeader.find_all('span')
                                               
        for i in range(1,len(valueRows)):
            colText = headerRows[-i].text
            if colText.find('/') > -1:
                _m,_d,year = headerRows[-i].text.split('/')
                value = float(valueRows[-i].text.replace(',',''))       
                print('{0}, {1}'.format(year, value))
            else:
                break
    
    
    

if __name__ == '__main__':
#     manualFinancials('EBIT')
#     manualFinancials('Normalized EBITDA')
#     manualBalance('Ordinary Shares Number')
#     manualBalance('Total Debt')
#     manualBalance('Total Liabilities Net Minority Interest')
#     manualBalance('Invested Capital')
#     spezManualBalance("Stockholders' Equity")

    
#     getBalance(6, 'Total Asset')
#     getFinancials(15, 'Revenue')
#     getFinancials(4, 'Net Income')

    totalBalance()

