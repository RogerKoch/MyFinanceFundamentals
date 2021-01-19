




#!/usr/bin/python
# -*- coding: UTF-8 -*-

from bs4 import BeautifulSoup
from .Isin2Ticker import getMappingResult
import mysql.connector
import requests



mydb = mysql.connector.connect(
  host="localhost",
  user="yourusername",
  password="yourpassword"
)


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


def finanzen_Data(fileName, url, searchExchange, setExchange):
    f = open(fileName +".csv", "w")
    basicLine = '{0};{1};{2}\n'

    pageList = pageLinks(url)
    instrumentInfos = {}

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

    if len(searchExchange) > 0:
        list_of_groups = list(chunks(list(instrumentInfos.keys()), 90))
        for group in list_of_groups:
            mappingRes = getMappingResult(group, searchExchange)
            for key in mappingRes.keys():
                isinList = mappingRes.get(key)
                if len(setExchange) > 0:
                    instrumentInfos.get(key).append(isinList[0] +"."+ setExchange)
                else:
                    instrumentInfos.get(key).append(isinList[0])
                instrumentInfos.get(key).append(isinList[2])
                instrumentInfos.get(key).append(isinList[3])
    else:
        for isin in instrumentInfos.keys():
            instrumentInfos.get(isin).append('')
            instrumentInfos.get(isin).append('')
            instrumentInfos.get(isin).append('')

    f = open(fileName +".csv", "w")
    basicLine = '{0};{1};{2};{3};{4};{5}\n'
    for line in instrumentInfos:
        instrLine = instrumentInfos.get(line)
        line = basicLine.format(instrLine[0],instrLine[1],instrLine[2],instrLine[3],instrLine[4],instrLine[5])
        f.write(line)

    f.flush()
    f.close()


if __name__ == '__main__':
    print('Start')
    # finanzen_Data('spi', 'https://www.finanzen.ch/index/liste/spi', 'SW', 'SW')
    # print('SPI FINISH')
    # finanzen_Data('s&p_500', 'https://www.finanzen.ch/index/liste/s&p_500', 'US', '')
    # print('S&P 500 FINISH')
    #finanzen_Data('dax', 'https://www.finanzen.ch/index/liste/dax', 'GR', 'DE')
    # print('DAX FINISH')
    # finanzen_Data('dowJones', 'https://www.finanzen.ch/index/liste/dow_jones', 'US', '')
    # print('DOWJONES FINISH')
    # finanzen_Data('nasdaq100', 'https://www.finanzen.ch/index/liste/nasdaq_100', 'US', '')
    # print('NASDAQ100 FINISH')
    # finanzen_Data('cac40', 'https://www.finanzen.ch/index/liste/cac_40', 'FP', 'PA')
    # print('CAC40 FINISH')
    # finanzen_Data('euronext100', 'https://www.finanzen.ch/index/liste/euronext_100', '', '')
    # print('EURONEXT FINISH')
    # finanzen_Data('ftse100', 'https://www.finanzen.ch/index/liste/ftse_100', 'LN', 'L')
    # print('FTSE100 FINISH')


