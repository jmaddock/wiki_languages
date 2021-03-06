import config
import os
import argparse
import datetime
import pandas as pd
import numpy as np

SCRIPT_DIR = os.path.abspath(__file__)

## read file and return pd dataframe
## if lang is EN or file it too large read w/ iterator
def read_wiki_edits_file(infile,lang=None):
    if lang == 'en':
        tp = pd.read_csv(infile,na_values={'title':''},keep_default_na=False,dtype={'title': object},iterator=True,chunksize=1000)
        df = pd.concat(tp, ignore_index=True)
    else:
        try:
            df = pd.read_csv(infile,na_values={'title':''},keep_default_na=False,dtype={'title': object})
        except MemoryError:
            log('file too large, importing with iterator...')
            tp = pd.read_csv(infile,na_values={'title':''},keep_default_na=False,dtype={'title': object},iterator=True,chunksize=1000)
            df = pd.concat(tp, ignore_index=True)
    return df

def log(text,log_file=None):
    print('[%s] %s' % (str(datetime.datetime.now().time())[:-7],text))
    if log_file:
        write_log(text)

def clean_en():
    fname = os.path.join(config.ROOT_PROCESSED_DIR,'en',config.EDIT_COUNTS)
    df = pd.read_csv(fname,na_values={'title':''},keep_default_na=False,dtype={'title': object})
    df = df.set_index('page_id',drop=False)
    df = df.set_value(2474652, 'title', 'Sam Vincent (disambiguation)')
    df = df.set_value(1877838, 'title', 'Bugatti Chiron (disambiguation)')
    df = df.drop([1826283,27902244])
    df.to_csv(fname,na_rep='NaN',encoding='utf-8')

def drop_cols(infile,outfile):
    df = pd.read_csv(infile,na_values={'title':''},keep_default_na=False,dtype={'title': object})
    columns = ['page_id_1','len_1','num_editors_1','tds_1','lang','page_id_0','len_0','num_editors_0','tds_0','ratio','editor_ratio']
    df.to_csv(outfile,na_rep='NaN',encoding='utf-8',columns=columns,index=False)

def drop1_editors(infile,outfile):
    df = pd.read_csv(infile,na_values={'title':''},keep_default_na=False,dtype={'title': object})
    df = df.loc[(df['num_editors_1'] > 1) & (df['num_editors_0'] > 1)]
    assert len(df.loc[(df['num_editors_1'] < 2) & (df['num_editors_0'] < 2)]) == 0
    df.to_csv(outfile,na_rep='NaN',encoding='utf-8',index=False)

def drop_n_outliers(infile,outfile,n):
    df = pd.read_csv(infile,na_values={'title':''},keep_default_na=False,dtype={'title': object})
    df = df.sort_values(['len_1'],ascending=True)
    result = df.head(len(df)-int(n))
    assert len(df) == len(result) + int(n)
    assert df.iloc[-2].equals(result.iloc[-1])
    result.to_csv(outfile,na_rep='NaN',encoding='utf-8',index=False)

def shuffle_and_split(infile,outfile,n):
    df = pd.read_csv(infile,na_values={'title':''},keep_default_na=False,dtype={'title': object})
    df = df.sample(frac=1).reset_index(drop=True)
    i = 1
    for chunk in np.array_split(df, int(n)):
        new_outfile = outfile.replace('.csv','')
        new_outfile = '{0}{1}.csv'.format(new_outfile,i)
        i += 1
        chunk.to_csv(new_outfile,na_rep='NaN',encoding='utf-8',index=False)

def job_script(args):
    # create the job script file, passed in command line params with -j flag
    f = open(args.job_script,'w')
    # get a list of language dirs if lang isn't specified
    langs = [name for name in os.listdir(config.ROOT_PROCESSED_DIR) if (os.path.isdir(os.path.join(config.ROOT_PROCESSED_DIR,name)) and 'combined' not in name)]
    for l in langs:
        infile = os.path.join(config.ROOT_PROCESSED_DIR,l,config.EDIT_COUNTS)
        outfile = os.path.join(config.ROOT_PROCESSED_DIR,l,config.EDIT_COUNTS_DROP1)
        out = 'python3 {0} -i {1} -o {2}'.format(SCRIPT_DIR,infile,outfile)
        if args.drop1:
            out = out + ' --drop1'
        out = out + '\n'
        print(out)
        f.write(out)
        
def main():
    parser = argparse.ArgumentParser(description='process wiki data')
    parser.add_argument('--clean_en',
                        action='store_true',
                        help='script for cleaning EN edit counts so it passes tests')
    parser.add_argument('--drop_cols',
                        action='store_true',
                        help='drop columns not required for models (titles, etc.)')
    parser.add_argument('--drop1',
                        action='store_true',
                        help='drop single pages with single editors and edits')
    parser.add_argument('--ss',
                        action='store_true',
                        help='shuffle and split the dataset for creating persistent random samples')
    parser.add_argument('--drop_outliers',
                        action='store_true',
                        help='drop a single outlier from the EN edit counts in order to reach model convergence')
    parser.add_argument('-i','--infile',
                        help='input file path')
    parser.add_argument('-o','--outfile',
                        help='ouput file path')
    parser.add_argument('-n','--num',
                        help='number of outliers to drop or number of splits')
    parser.add_argument('-j','--job_script',
                        help='create a job script')
    args = parser.parse_args()
    if args.job_script:
        job_script(args)
    else:
        if args.clean_en:
            clean_en()
        if args.drop_cols:
            drop_cols(args.infile,args.outfile)
        if args.drop1:
            drop1(args.infile,args.outfile)
        if args.ss:
            shuffle_and_split(args.infile,args.outfile,args.num)
        if args.drop_outliers:
            drop_n_outliers(args.infile,args.outfile,args.num)

if __name__ == "__main__":
    main()
