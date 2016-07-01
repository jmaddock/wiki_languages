import requests
import argparse
import pandas as pd

def download_from_web():
    base_url = r'http://wikistats.wmflabs.org/api.php'
    params = {
        'action':'dump',
        'table':'wikipedias',
        'format':'csv'
    }
    url = requests.Request('GET',base_url,params=params).prepare().url
    df = pd.read_csv(url,index_col=False)
    return df

def create_ordered_lang_list(df,n=None,outfile=None):
    df = df.sort_values(by='edits',ascending=False)
    if n:
        df = df.head(n=n)
    df = df['prefix'].to_frame('lang')
    if outfile:
        df.to_csv(outfile,index=False)
    return df

if __name__ == "__main__":
    description = 'Collect a list of n languages from the wikistats and export as a CSV'
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('-o','--outfile')
    parser.add_argument('-n','--num_rows',type=int)
    args = parser.parse_args()
    df = download_from_web()
    create_ordered_lang_list(df,args.num_rows,args.outfile)

        
