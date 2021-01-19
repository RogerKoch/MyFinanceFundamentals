
import json
import urllib.request

'''
See https://www.openfigi.com/api for more information.
'''

openfigi_apikey = '21a1da4b-d825-4d7b-9a6f-984cb994ee32'

def getMappings(mappingList):

    handler = urllib.request.HTTPHandler()
    opener = urllib.request.build_opener(handler)
    openfigi_url = 'https://api.openfigi.com/v2/mapping'
    request = urllib.request.Request(openfigi_url, data=bytes(json.dumps(mappingList), encoding='utf-8'))
    request.add_header('Content-Type','application/json')
    if openfigi_apikey:
        request.add_header('X-OPENFIGI-APIKEY', openfigi_apikey)
    request.get_method = lambda: 'POST'
    connection = opener.open(request)
    if connection.code != 200:
        raise Exception('Bad response code {}'.format(str(connection.status_code)))
    return json.loads(connection.read().decode('utf-8'))


def getMappingResult(group, searchExchange):
    mappingList=[]
    for isin in group:
        mappingList.append({'idType': 'ID_ISIN', 'idValue': isin, "exchCode": searchExchange})

    mappingResultJson = getMappings(mappingList)
    mappingResult= {}
    for job, result in zip(mappingList, mappingResultJson):
        isin = job['idValue']
        for d in result.get('data', []):            
            ticker = d['ticker']
            exchCode=d['exchCode']
            securityType = d['securityType']
            marketSector = d['marketSector']
            mappingResult[isin]=[ticker, exchCode, securityType, marketSector]
    return mappingResult

