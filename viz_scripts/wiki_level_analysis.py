import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
import argparse
import pandas as pd
import datetime
import utils


def get_wiki_size(infile,outfile=None):
    df = pd.read_csv(infile,na_values={'title':''},keep_default_na=False,dtype={'title': object})
    df = df.groupby('lang').page_id_1.nunique().to_frame('order_by')
    df = df.reset_index(level=0)
    df = df.rename(columns={'index':'lang'})
    if outfile:
        df.to_csv(outfile,na_rep='NaN',encoding='utf-8',index=False)
    return df

def get_wiki_age(outfile=None):
    langs = [name for name in os.listdir(config.ROOT_PROCESSED_DIR) if (os.path.isdir(os.path.join(config.ROOT_PROCESSED_DIR,name)) and 'combined' not in name and 'simple' not in name)]
    result = pd.DataFrame({'lang':[],'order_by':[]})
    for l in langs:
        infile = os.path.join(config.ROOT_PROCESSED_DIR,l,config.COMBINED_RAW_EDITS)
        print(infile)
        try:
            df = utils.read_wiki_edits_file(infile,l)
        except OSError:
            utils.log('no file: {0}'.format(infile))
            continue
        df['ts'] = pd.to_datetime(df['ts'],format="%Y-%m-%d %H:%M:%S")
        ts_min = df['ts'].min()
        ts_max = df['ts'].max()
        print(result)
        result = result.append(pd.DataFrame({'lang':[l],'order_by':[ts_max-ts_min]}))
    if outfile:
        result.to_csv(outfile,na_rep='NaN',encoding='utf-8',index=False)
    return result
        
def main():
    parser = argparse.ArgumentParser(description='process wiki data')
    parser.add_argument('-i','--infile',
                        help='input .csv path of combined edit counts.  use with --analysis_type = size')
    parser.add_argument('-o','--outfile',
                        required=True,
                        help='output .csv path')
    parser.add_argument('-a','--analysis_type',
                        required=True,
                        choices=['size','age'],
                        help='get wiki age or wiki size')
    args = parser.parse_args()
    if args.analysis_type == 'size':
        get_wiki_size(args.infile,args.outfile)
    elif args.analysis_type == 'age':
        get_wiki_age(args.outfile)

if __name__ == "__main__":
    main()


