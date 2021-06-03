import pandas as pd
import csv
import requests
import os
import sys
import getopt



class GeoTagger(object):
    """Geographic Tagger for documents in spanish"""
    def __init__(self, data, text_col, vec_col=None,):
        super(GeoTagger, self).__init__()
        self.df = data
        self.geodf = None
        self.text_col = text_col

    def tag(self):
        """Enrich data with toponys and coordinates using geoparseMX"""
        #print('Geoparsing ...')
        self.df[self.text_col].apply(lambda x: self.geoparseMX(x))
        #print('DONE')

    def get_geotags(self):
        """Enrich data with toponys and coordinates using geoparseMX"""
        #print('Geoparsing data ...')
        geodf = []
        for row in self.df.itertuples(index=True):
            for (entity,coordinates,geotags) in self.geoparseMX(row[-1]):
                r = row+(entity,list(coordinates),geotags)
                geodf.append(r)
                print(r)
                # print('"{}"\t"{}"\t"{}"\t"{}"\t"{}"\t"{}"\t"{}"\t"{}"\t"{}"\t"{}"\n').format(
                #     r[0],r[1],r[2],r[3],r[4],r[5],r[6],r[7],r[8],r[9])
        self.geodf = pd.DataFrame(geodf)

    @staticmethod
    def geoparseMX(text):
        """Geoparsing service for mexican spanish
           see http://geoparsing.geoint.mx/mx/info/
        """
        geoparser_url = "http://geoparsing.geoint.mx/ws/"
        data = dict({"text" : text})
        #print(text[:250])
        try:
            response = requests.post(geoparser_url, json = data, headers={"Content-Type":"application/json"})
            jresponse = response.json()
            for e in jresponse['entities']:
                entity = e['entity']
                #print('entity', e['entity'])
                place = e['nominatim'][0]
                geotags = place['address']
                #print('geotags',geotags)
                try:
                    coords = [place['lon'], place['lat']]
                except Exception as e:
                    coords = ""
                #print('coords',coords)
                yield (entity,coords,geotags)
        except Exception as e:
            yield ("", "", "")

def load_data(csv_path):
    """Read a single csv file"""
    #print('loading file ...')
    df = pd.read_csv(csv_path, sep='\t', header=0, index_col=0,)
    df.drop('diff', inplace=True, axis=1)
    df.drop('abstract', inplace=True, axis=1)
    df = df.drop_duplicates()
    df = df.dropna()
    #print('DONE')
    return df

if __name__ == '__main__':

    usage = 'Usage: vspace.py -i <input> -o <output> -h <help>'
    argv = sys.argv[1:]
    if not argv:
        print(usage)
        sys.exit(2)

    try:
        opts, args = getopt.getopt(argv,
                                   'i:o:h',
                                   ['input_docs',
                                    'out_dir',
                                    'help'])
    except getopt.GetoptError:
        print(usage)
        sys.exit(2)

    for opt, arg in opts:
        if opt in ('-h', '--help'):
            print(usage)
            sys.exit()
        elif opt in ('-i', '--input'):
            input_documents = arg
        elif opt in ('-o', '--output'):
            output = arg if arg else './'

    data = load_data(input_documents)
    gtagger = GeoTagger(data,'text', vec_col=None)
    gtagger.get_geotags()
    # print(gtagger.geodf)
    gtagger.geodf.to_csv(output,
        header=False,
        sep='\t',        
        index=False,
        quoting=csv.QUOTE_ALL,)
