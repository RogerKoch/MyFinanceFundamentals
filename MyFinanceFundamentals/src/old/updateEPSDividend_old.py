#!/usr/bin/python
# -*- coding: UTF-8 -*-

from getDatabaseConnection import DatabaseManager
import finanzen_fundamentals.stocks as ff


def updateEPSDividend():
    
    db = DatabaseManager()
    mydb = db.setDBConnection('stockInfo')
    myCursor = mydb.cursor()    
            
    #Get all ISINs
    insertQuery = ("""Call stockInfo.spInsDivFigures (%s, %s, %s, %s)""")
    insertCursor = mydb.cursor()
    
    myCursor.execute('SELECT isin, financeKey FROM stockInfo.BasicData LIMIT 10')
    allIsins = myCursor.fetchall()
    for sqlRow in allIsins:    
        isin = sqlRow[0]
        finanzenKey = sqlRow[1]
        fundamentals = None
        try:
            fundamentals = ff.get_fundamentals(finanzenKey, output = "dict")
        except ValueError:
            print('ISIN Not FOUND: {0}, {1}'.format(isin,finanzenKey))
            try:
                finanzenKey = ff.search_stock(finanzenKey, limit = 1)[0][1]
                fundamentals = ff.get_fundamentals(finanzenKey, output="dict")
            except IndexError:
                print('NO NAME FOUND FOR: {0}, {1}'.format(isin, finanzenKey))
            except AttributeError:
                print('NO NAME FOUND FOR: {0}, {1}'.format(isin, finanzenKey))
            except ValueError:
                print('NEW LONGNAME NOT FOUND: {0}, {1}'.format(isin,finanzenKey))
    
        try:
            if fundamentals is not None:
                if 'Quotes' in fundamentals:
                    quotes = fundamentals.get('Quotes')
                    if 'Ergebnis je Aktie (unverwässert, nach Steuern)' in quotes:
                        epsDil = quotes.get('Ergebnis je Aktie (unverwässert, nach Steuern)')
                        if epsDil is not None:
                            for key in epsDil.keys():
                                insertData=(isin, 'EPS Diluted', key, epsDil.get(key))
                                insertCursor.execute(insertQuery, insertData)
                            mydb.commit()
                    if 'Ergebnis je Aktie (verwässert, nach Steuern)' in quotes:
                        epsBasic= quotes.get('Ergebnis je Aktie (verwässert, nach Steuern)')
                        if epsBasic is not None:
                            for key in epsBasic.keys():
                                insertData=(isin, 'EPS Basic', key, epsBasic.get(key))
                                insertCursor.execute(insertQuery, insertData)                                
                            mydb.commit()
                    if 'Dividende je Aktie' in quotes:
                        divid = quotes.get('Dividende je Aktie')
                        if divid is not None:
                            for key in divid.keys():
                                if divid.get(key) is None:
                                    div = 0
                                else:
                                    div = divid.get(key)
                                insertData=(isin, 'Dividend', key, div)
                                insertCursor.execute(insertQuery, insertData)                                
                            mydb.commit()

        except TypeError:
            print('ERROR IN TYPE: {0}, {1}'.format(isin,finanzenKey))
    insertCursor.close()  


if __name__ == '__main__':
    updateEPSDividend()
