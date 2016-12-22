import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
import utils
import argparse
from concurrent.futures import ProcessPoolExecutor
from functools import partial
import gc

WORKERS = 4

def simulate_df(observed_df):
    # rename language labels from simulated dataset
    sim_df = observed_df.rename(columns={'lang':'original_lang'})
    # shuffle the rows in the simulated dataset and reset the index
    sim_df = sim_df.sample(frac=1).reset_index(drop=True)
    # create a columns of only language labels (this ensures that they have the correct proportion)
    lang = observed_df[['lang']]
    # merge the shuffled simulated dataset with the ordered data labels
    # this effectively shuffles the language labels
    sim_df = sim_df.merge(lang,left_index=True,right_index=True)
    return sim_df    

def main(n,outfile,observed_df,file_format):
    utils.log(n)
    simulated_df = simulate_df(observed_df)
    if file_format == 'csv':
        outfile_name = "{0}_{1}.csv".format(outfile,n)
        simulated_df.to_csv(outfile_name,index=False)
    if file_format == 'dta':
        outfile_name = "{0}_{1}.dta".format(outfile,n)
        simulated_df.to_stata(outfile_name,write_index=False)
        #if i % 10 == 0:
    gc.collect()
    utils.log('finished {0}'.format(n))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='process wiki data')
    parser.add_argument('-i','--infile',
                        help='input file path')
    parser.add_argument('-o','--outfile',
                        help='ouput file path')
    parser.add_argument('-n','--num',
                        type=int,
                        help='number of simulated datasets to create')
    parser.add_argument('-f','--file_format',
                        choices=['csv','dta'],
                        help='output .csv or .dta file')
    parser.add_argument('-w','--workers',
                        type=int,
                        default=WORKERS,
                        help='the number of workers to use for processing')
    args = parser.parse_args()
    # read the csv containing observed values
    observed_df = pd.read_csv(args.infile)
    utils.log('loaded file {0}'.format(args.infile))
    # calculate the optimal chunk size based on number of workers specified
    chunksize = int(int(args.num)/args.workers)
    utils.log('WORKERS: {0}\nCHUNK SIZE: {1}\nTOTAL FILES: {2}'.format(args.workers,chunksize,args.num))
    # create a partial function of main() to use the loaded df, outfile path
    main_func = partial(main,
                        outfile=args.outfile,
                        observed_df=observed_df,
                        file_format=args.file_format)
    # create a process pool and execute main
    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        executor.map(main_func,range(args.num))
    utils.log('processed {0} files'.format(args.num))
