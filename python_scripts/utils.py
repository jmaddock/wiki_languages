import config
import os
import argparse
import pandas as pd

def clean_en():
    fname = os.path.join(config.ROOT_PROCESSED_DIR,'en',config.EDIT_COUNTS)
    df = pd.read_csv(fname,na_values={'title':''},keep_default_na=False,dtype={'title': object})
    df = df.set_index('page_id',drop=False)
    df = df.set_value(2474652, 'title', 'Sam Vincent (disambiguation)')
    df = df.set_value(1877838, 'title', 'Bugatti Chiron (disambiguation)')
    df = df.drop([1826283,27902244])
    df.to_csv(fname,na_rep='NaN',encoding='utf-8')

def drop_cols(infile,outfile):
    df = pd.read_csv(fname,na_values={'title':''},keep_default_na=False,dtype={'title': object})
    print(df)

def main():
    parser = argparse.ArgumentParser(description='process wiki data')
    parser.add_argument('--clean_en',action='store_true')
    parser.add_argument('--drop_cols',action='store_true')
    parser.add_argument('-i','--infile')
    parser.add_argument('-o','--outfile')
    parser.add_argument('-l','--lang')
    args = parser.parse_args()
    if args.clean_en:
        clean_en()
    if args.drop_cols:
        if args.lang:
            infile = os.path.join(config.ROOT_PROCESSED_DIR,args.lang,config.COMBINED_EDIT_RATIOS)
            outfile = os.path.join(config.ROOT_PROCESSED_DIR,args.lang,config.COMBINED_EDIT_RATIOS_NO_TITLES)
        elif args.infile or args.outfile:
            infile = args.infile
            oufile = args.outfile
        else:
            basic.log('please provide --lang or --infile and --outfile')
        drop_cols(infile,outfile)
        
if __name__ == "__main__":
    main()
