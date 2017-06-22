import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
import utils
import argparse

SCRIPT_DIR = os.path.abspath(__file__)

def combine_threshold_files(file_list):
    result_df = pd.DataFrame()
    for f in file_list:
        utils.log('adding file {0}'.format(f))
        result_df = result_df.append(pd.read_csv(f,na_values={'title':''},keep_default_na=False))
    utils.log('{0} article/talk pairs'.format(len(result_df)))
    return result_df

def create_file_list(base_dir,num):
    file_list = [os.path.join(base_dir,f) for f in os.listdir(base_dir) if strip_filename(f) == str(num)]
    utils.log('found {0} file for bin {1}'.format(len(file_list),num))
    return file_list

def strip_filename(filename):
    return filename.split('_')[-1].strip('.csv')

def job_script(args):
    # create the job script file, passed in command line params with -j flag
    f = open(args.job_script,'w')
    max_num = 0
    for filename in os.listdir(args.indir):
        num = int(strip_filename(filename))
        if num > max_num:
            max_num = num
    for i in range(max_num + 1):
        out = 'python3 {0} -i {1} -o {2} -n {3}\n'.format(SCRIPT_DIR,
                                                          args.indir,
                                                          args.outdir,
                                                          i)
        print(out)
        f.write(out)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='process wiki data')
    parser.add_argument('-i', '--indir',
                        help='path to directory containing edit counts thresholded by relative date')
    parser.add_argument('-o', '--outdir',
                        help='path to directory for output files')
    parser.add_argument('-j', '--job_script',
                        help='create a job script for processing on hyak')
    parser.add_argument('-n', '--time_series_bin_num',
                        help='the number of the time series bin to combine')
    args = parser.parse_args()
    if args.job_script:
        job_script(args)
    else:
        file_list = create_file_list(base_dir=args.indir,num=args.time_series_bin_num)
        utils.log('combining bin {0} files'.format(args.time_series_bin_num))
        result_df = combine_threshold_files(file_list)
        outfile = '{0}_{1}.csv'.format(args.outdir,args.time_series_bin_num)
        utils.log('writing file {0}'.format(outfile))
        result_df.to_csv(outfile,index=False)
