import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
import datetime
import utils
import argparse
import matplotlib.pyplot as plt
import matplotlib
from ggplot import *
import numpy as np

matplotlib.style.use('ggplot')

def format_table(simulated_infile,observed_infile,labels_file):
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
        # read the labels file and import b as an object
    labels = pd.read_table(labels_file,header=1,dtype={'b':object})
    labels = labels.rename(columns={'Unnamed: 0':'lang'})
    # merge labels with results based on observed coefs (b)
    result_df = result_df.merge(labels,on='b')
    # eliminate all non-language coefficients
    result_df = result_df.loc[result_df['lang'].str.len() == 2]
    # make sure columns are numeric
    result_df['50%'] = pd.to_numeric(result_df['50%'])
    result_df['b'] = pd.to_numeric(result_df['b'])
    # drop english reference
    result_df = result_df.loc[result_df['lang'] != 'en']
    # reset index for graphing
    result_df = result_df.reset_index()
    return result_df

def make_graph(result_df,graph_type):
    fig = plt.figure(figsize=(4,5))
    result_df = result_df.sort_values(by='b')
    if graph_type == 'bars':
        lw=4
        plt.errorbar(x=result_df['50%'],
                 y=np.arange(len(result_df)),
                 xerr=[result_df['50%'], result_df['b'].subtract(result_df['50%'])],
                 fmt='none',
                 ecolor='grey',
                 lw=2)
    else:
        plt.plot(result_df['b'],
                 np.arange(len(result_df)),
                 '.')
        lw=1
    plt.errorbar(x=result_df['50%'],
                     y=np.arange(len(result_df)),
                     xerr=[result_df['50%'].subtract(result_df['min']),
                           result_df['max'].subtract(result_df['50%'])],
                     lw=lw,
                     ecolor='gray',
                     fmt='none')
    plt.yticks(np.arange(len(result_df)), result_df['lang'])
    plt.ylim(-1, len(result_df))
    plt.ylabel('Language Edition')
    plt.xlabel('Coefficient')
    plt.tight_layout()
    return fig

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
    parser.add_argument('--show_graph',
                        action='store_true',
                        help='flag to plot graph in new window')
    parser.add_argument('-t','--graph_type',
                        choices=['bars','points'],
                        help='')
    args = parser.parse_args()
    result_df = format_table(simulated_infile=args.simulated_infile,
                             observed_infile=args.actual_infile,
                             labels_file=args.labels_file)
    fig = make_graph(result_df,args.graph_type)
    if args.outfile:
        fig.savefig(args.outfile)
    if args.show_graph:
        plt.show()

if __name__ == "__main__":
    main()
