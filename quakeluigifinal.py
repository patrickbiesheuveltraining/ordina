import luigi
from luigi import Task
import pandas as pd
import requests
import json

class Import(Task):
    def output(self):
        return luigi.LocalTarget('resultimport.csv')
    
    def run(self):
        data = pd.read_csv('database.csv')
        data2 = data[['Date','Time','Latitude', 'Longitude','Magnitude']]
        #print(self.output())
        #data2.to_csv('test.csv')
        with self.output().open('w') as csv:
            csv.write(data2.to_csv())

class Clean(Task):
    def requires(self):
        return Import()
    
    def output(self):
        return luigi.LocalTarget('resultclean.csv')
    
    def run(self):
        df = pd.read_csv(self.input())
        df = df.sort_values(by='Magnitude', ascending=False)
        df = df.head(10)
        with self.output().open('w') as csv:
            csv.write(df.to_csv())

class Enrich(Task):
    def requires(self):
        return Clean()
    
    def output(self):
        return luigi.LocalTarget('resultenrich.csv')
    
    def run(self):
        df = pd.read_csv(self.input())
        df['Country'] = df.apply(lambda x: getCountry(x['Latitude'], x['Longitude']), axis=1)
        with self.output().open('w') as csv:
            csv.write(df.to_csv())

class Export(Task):
    def requires(self):
        return Enrich()
    
    def output(self):
        return luigi.LocalTarget('resultexport.txt')
    
    def run(self):
        df = pd.read_csv(self.input())
        print(df)
        with self.output().open('w') as f:
            f.write('Succesfully ran the pipeline!\n')



def getCountry(lati, longi):
    url = 'http://api.geonames.org/countryCodeJSON'

    params = dict(
    formatted='true',
    lat = lati,
    lng = longi,
    username = 'pbtraining',
    style = 'full')

    resp = requests.get(url=url, params=params)
        
    if not resp.text.startswith('{"status"'):
        jsonresp = resp.json()
        return jsonresp['countryName']
    else:
        return 'None'
    
if __name__ == '__main__':
    luigi.run(['Export', '--local-scheduler'])
