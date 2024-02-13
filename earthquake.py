import pandas as pd
import requests
import json

def getCountry(lati, longi):
    url = 'http://api.geonames.org/countryCodeJSON'

    params = dict(
    formatted='true',
    lat = lati,
    lng = longi,
    username = 'pbtraining',
    style = 'full')

    resp = requests.get(url=url, params=params)
        
    #if not resp.text.startswith('{"status"'):
    if 'countryName' in resp.keys():
        jsonresp = resp.json()
        return jsonresp['countryName']
    else:
        return 'None'
   


data = pd.read_csv('database.csv')
data2 = data[['Date','Time','Latitude', 'Longitude','Magnitude']]


data10 = data2.sort_values(by='Magnitude', ascending=False)
data11 = data10.head(10)


data11['Country'] = data11.apply(lambda x: getCountry(x['Latitude'], x['Longitude']), axis=1)
#data10.head(10)
print(data11)

