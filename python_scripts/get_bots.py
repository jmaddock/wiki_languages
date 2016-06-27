import argparse
import requests
import config
import utils
import pandas as pd

CMLIMIT = 500

def get_bot_list_from_web(outfile=None):
    base_url = r'https://en.wikipedia.org/w/api.php'
    params = {
        'cmtitle':'Category:All_Wikipedia_bots',
        'action':'query',
        'list':'categorymembers',
        'cmsort':'sortkey',
        'cmlimit':CMLIMIT,
        'format':'json'
    }
    df = pd.DataFrame()
    lastContinue = {'continue': ''}
    while True:
        params.update(lastContinue)
        r = requests.get(base_url, params=params).json()
        if 'error' in r:
            raise Error(r['error'])
        if 'warnings' in r:
            print(r['warnings'])
        if 'query' in r:
            df = df.append(traverse_query(r['query']['categorymembers']))
            utils.log('processed {0} bot names'.format(len(df)))
        if 'continue' not in r:
            if outfile:
                df.to_csv(outfile,encoding='utf-8',index=False)
            return df
        lastContinue = r['continue']

def traverse_query(response):
    df = pd.DataFrame()
    for x in response:
        bot_name = x['title'].split(':',1)[-1].replace('"',config.QUOTE_ESCAPE_CHAR).strip()
        df = df.append([{'bot_name':bot_name}])
    return df
        
if __name__ == "__main__":
    description = 'Collect a list of bots from Category:All_Wikipedia_bots and output as a CSV'
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('-o','--outfile')
    args = parser.parse_args()
    if args.outfile:
        get_bot_list_from_web(args.outfile)
