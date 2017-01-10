import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
import datetime
import utils
import argparse

def make_table(simulated_infile,observed_infile,labels_file=None):
    # read the all of the simulated coefs
    simulated_df = pd.read_csv(simulated_infile)
    # get summary stats of simulated distribution
    result_df = simulated_df.describe().transpose()
    # read the actual observed coefs
    observed_df = pd.read_table(observed_infile,index_col=0,header=1).transpose()
    # change the index
    result_df['vars'] = result_df.index
    observed_df['vars'] = observed_df.index
    # join the two dfs
    result_df = result_df.merge(observed_df[['vars','b']],on='vars')
    if labels_file:
        # read the labels file and import b as an object
        labels = pd.read_table(labels_file,header=1,dtype={'b':object})
        labels = labels.rename(columns={'Unnamed: 0':'lang'})
        # merge labels with results based on observed coefs (b)
        result_df = result_df.merge(labels,on='b')
        # eliminate all non-language coefficients
        result_df = result_df.loc[result_df['lang'].str.len() == 2]
    result_df['b'] = pd.to_numeric(result_df['b'])
    result_df['num_sd'] = result_df['b'].subtract(result_df['mean']).divide(result_df['std'])
    result_df = result_df.loc[result_df['lang'] != 'en']
    result_df = result_df.rename(columns={'50%':'median',
                                          'b':'observed_coef'})
    print(result_df)
    return result_df

def main():
    parser = argparse.ArgumentParser(description='process wiki data')
    parser.add_argument('-o','--outfile',
                        help='file path for sorted csv output')
    parser.add_argument('-s','--simulated_infile',
                        help='file with simulated coefs')
    parser.add_argument('-a','--actual_infile',
                        help='file with observed (actual) coefs')
    parser.add_argument('-l','--labels_file',
                        help='file with labels and observed coef values')
    args = parser.parse_args()
    result_df = make_table(simulated_infile=args.simulated_infile,
                           observed_infile=args.actual_infile,
                           labels_file=args.labels_file)
    result_df.to_csv(args.outfile,index=False)

if __name__ == "__main__":
    main()
