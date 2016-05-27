import basic
import config
import os
import argparse
import pandas as pd
import datetime


def get_wiki_size(infile,outfile):
    df = pd.read_csv(infile,na_values={'title':''},keep_default_na=False,dtype={'title': object})
    df = df.groupby('lang').page_id.nunique().to_frame('num_pages')
    df = df.reset_index(level=0)
    df = df.rename(columns={'index':'lang'})
    df.to_csv(outfile,na_rep='NaN',columns=columns,encoding='utf-8')
    return df

def get_wiki_age(outfile):
    langs = [name for name in os.listdir(config.ROOT_PROCESSED_DIR) if (os.path.isdir(os.path.join(config.ROOT_PROCESSED_DIR,name)) and 'combined' not in name and 'simple' not in name)]
    result = pd.DataFrame({'lang':[],'age':[]})
    for l in langs:
        infile = os.path.join(config.ROOT_PROCESSED_DIR,l,config.COMBINED_RAW_EDITS)
        try:
            if l == 'en':
                tp = pd.read_csv(infile,na_values={'title':''},keep_default_na=False,dtype={'title': object},iterator=True,chunksize=1000)
                df = pd.concat(tp, ignore_index=True)
            else:
                try:
                    df = pd.read_csv(infile,na_values={'title':''},keep_default_na=False,dtype={'title': object})
                except MemoryError:
                    basic.log('file too large, importing with iterator...')
                    tp = pd.read_csv(infile,na_values={'title':''},keep_default_na=False,dtype={'title': object},iterator=True,chunksize=1000)
                    df = pd.concat(tp, ignore_index=True)
        except OSError:
            basic.log('no file: {0}'.format(infile))
            continue
        df['ts'] = pd.to_datetime(df['ts'],format="%Y-%m-%d %H:%M:%S")
        ts_min = df['ts'].min()
        ts_max = df['ts'].max()
        result = result.append(pd.DataFrame({'lang':[l],'age':[ts_max-ts_min]}))
    result.to_csv(outfile,na_rep='NaN',columns=columns,encoding='utf-8')
    return result
        
def main():
    parser = argparse.ArgumentParser(description='process wiki data')
    parser.add_argument('-i','--infile')
    parser.add_argument('-o','--outfile')
    parser.add_argument('-a','--analysis_type')
    args = parser.parse_args()
    if args.analysis_type == 'size':
        get_wiki_size(args.infile,args.outfile)
    elif args.analysis_type == 'age':
        get_wiki_age(args.outfile)

if __name__ == "__main__":
    main()


