import config
import os
import argparse
import pandas as pd
import numpy as np

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

def drop1(infile,outfile):
    df = pd.read_csv(infile,na_values={'title':''},keep_default_na=False,dtype={'title': object})
    df = df.loc[(df['num_editors_1'] > 1) & (df['num_editors_0'] > 1)]
    assert len(df.loc[(df['num_editors_1'] < 2) & (df['num_editors_0'] < 2)]) == 0
    df.to_csv(outfile,na_rep='NaN',encoding='utf-8',index=False)

def shuffle_and_split(infile,outfile,n):
    df = pd.read_csv(infile,na_values={'title':''},keep_default_na=False,dtype={'title': object})
    df = df.sample(frac=1).reset_index(drop=True)
    i = 1
    for chunk in np.array_split(df, int(n)):
        new_outfile = outfile.replace('.csv','')
        new_outfile = '{0}{1}.csv'.format(new_outfile,i)
        i += 1
        chunk.to_csv(new_outfile,na_rep='NaN',encoding='utf-8',index=False)

def main():
    parser = argparse.ArgumentParser(description='process wiki data')
    parser.add_argument('--clean_en',action='store_true')
    parser.add_argument('--drop_cols',action='store_true')
    parser.add_argument('--drop1',action='store_true')
    parser.add_argument('--ss',action='store_true')
    parser.add_argument('-i','--infile')
    parser.add_argument('-o','--outfile')
    parser.add_argument('-n','--num')
    args = parser.parse_args()
    if args.clean_en:
        clean_en()
    if args.drop_cols:
        drop_cols(args.infile,args.outfile)
    if args.drop1:
        drop1(args.infile,args.outfile)
    if args.ss:
        shuffle_and_split(args.infile,args.outfile,args.num)
        
if __name__ == "__main__":
    main()
