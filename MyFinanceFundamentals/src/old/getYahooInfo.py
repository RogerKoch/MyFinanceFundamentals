#!/usr/bin/python
# -*- coding: UTF-8 -*-

import yfinance as yf


def main():
    ticker = yf.Ticker("M")
    ti = ticker.info
    print(ti)
    td = ticker.dividends
    print(td)
    
    for index, row in td.items():
        print(str(index) + '\t' + str(row))    
    
    er = ticker.earnings
    for index, row in er.iterrows():
        print(str(index) + '\t' + str(row))

    fi = ticker.financials
    for index, row in fi.iterrows():
        print(str(index) + '\t' + str(row))
    
    rr=ticker.history(start='2020-10-31', end='2020-12-13', interval='1d')
    for index, row in rr.iterrows():
        print(str(index) + '\t' + str(row.Close))
        dateString=index.strftime('%Y-%m-%d')
    

    shareInfo = [ti['previousClose'], ti['fiftyTwoWeekHigh'], ti['fiftyTwoWeekLow'], ti['trailingPE'], ti['bookValue'], ti['priceToBook'], ti['dividendRate'], ti['exDividendDate'], ti['industry'], ti['sector']]
    print(shareInfo)






if __name__ == '__main__':
    main()
