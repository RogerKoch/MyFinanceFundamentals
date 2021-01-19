#!/usr/bin/python
# -*- coding: UTF-8 -*-

import finanzen_fundamentals.stocks as ff
import xlrd


def main():



    f = open("Fundamentals_append.csv", "w")
    #f = open("Fundamentals.csv", "w")
    basicLine = '{0};{1};{2};{3};{4};{5}\n'

    xlFile = xlrd.open_workbook('Overview_Dividend_Investments.xlsx')
    xlSheet = xlFile.sheet_by_index(0)

    maxRows = xlSheet.nrows
    ranges = maxRows / 50

    for row_idx in range(xlSheet.nrows-1, xlSheet.nrows):
    #for row_idx in range(1, xlSheet.nrows):
        #row_idx=49
        found = True
        isin = str(xlSheet.cell_value(row_idx, 0))
        finanzenKey = str(xlSheet.cell_value(row_idx, 3))
        fundamentals = None
        try:
            fundamentals = ff.get_fundamentals(finanzenKey, output = "dict")
        except ValueError:
            found = False
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
                                line=basicLine.format(isin, 'EPS Diluted', key, epsDil.get(key), found, finanzenKey)
                                f.write(line)
                    if 'Ergebnis je Aktie (verwässert, nach Steuern)' in quotes:
                        epsBasic= quotes.get('Ergebnis je Aktie (verwässert, nach Steuern)')
                        if epsBasic is not None:
                            for key in epsBasic.keys():
                                line=basicLine.format(isin, 'EPS Basic', key, epsBasic.get(key), found, finanzenKey)
                                f.write(line)
                    if 'Dividende je Aktie' in quotes:
                        divid = quotes.get('Dividende je Aktie')
                        if divid is not None:
                            for key in divid.keys():
                                if divid.get(key) is None:
                                    div = 0
                                else:
                                    div = divid.get(key)
                                line=basicLine.format(isin, 'Dividend', key, div, found, finanzenKey)
                                f.write(line)
                    f.flush()
        except TypeError:
            print('ERROR IN TYPE: {0}, {1}'.format(isin,finanzenKey))




    f.close()

if __name__ == '__main__':
    main()
