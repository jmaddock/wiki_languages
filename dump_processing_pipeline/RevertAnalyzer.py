import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from collections import Counter
import pandas as pd
import datetime
import utils
import argparse

def get_reverts(df):
    # get the number of reverts
    df['reverts_1'] = df['len_1'].subtract(df['no_revert_len_1'])
    df['reverts_0'] = df['len_0'].subtract(df['no_revert_len_0'])
    return df

def write_file(df,outfile):
    columns = ['page_id_1','len_1','num_editors_1','tds_1','lang','page_id_0','len_0','num_editors_0','tds_0','ratio','editor_ratio','reverts_1','reverts_0','no_revert_len_1','no_revert_len_0']
    df.to_csv(outfile,na_rep='NaN',encoding='utf-8',columns=columns,index=False)


def main():
    parser = argparse.ArgumentParser(description='process wiki data')
    parser.add_argument('-i','--infile',
                        help='combined edit ratios csv from which to calculate edit ratios')
    parser.add_argument('-o','--outfile',
                        help='output csv')
    args = parser.parse_args()
    df = pd.read_csv(args.infile,na_values={'title':''},keep_default_na=False,dtype={'title': object})
    df = get_reverts(df)
    write_file(df,args.outfile)
        
if __name__ == "__main__":
    main()
