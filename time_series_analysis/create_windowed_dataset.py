import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
import utils
import argparse

SCRIPT_DIR = os.path.abspath(__file__)

def subtract_dfs(earlier,later):
    # specify columns that will be subtracted
    overlap_cols = ['len_1', 'len_0', 'no_revert_len_1', 'no_revert_len_0']
    # set multi-index for both pages
    earlier = earlier.set_index(['page_id_1', 'lang'])
    later = later.set_index(['page_id_1', 'lang'])
    # subtract earlier from later by multi-index
    result = later[overlap_cols].subtract(earlier[overlap_cols])
    # join meta data from later df
    result = result.join(later[['title', 'tds_1', 'tds_0', 'num_editors_1', 'num_editors_0', 'page_id_0']])
    # drop all pages without titles
    # pages without titles are talk pages that have been archived in the later dataset (and are therefore combined into the new page)
    result = result.loc[result['title'].notnull()]
    # join counts from later df with result df for rows that don't exist in earlier df
    result = result.loc[result['len_1'].isnull()].drop(overlap_cols,axis=1).join(later[overlap_cols],how='inner')
    return result

def strip_filename(filename):
    return filename.split('_')[-1].replace('.csv','')

def get_earlier_filename(filename):
    return '{0}_{1}.csv'.format(filename.split('_')[-2],int(strip_filename(filename))-1)

def job_script(args):
    # create the job script file, passed in command line params with -j flag
    f = open(args.job_script, 'w')
    out = ''
    for filename in os.listdir(args.base_in_dir):
        infile = os.path.join(args.base_in_dir, filename)
        outfile = os.path.join(args.base_out_dir, filename)
        if int(strip_filename(filename)) == 0:
            out = out + 'python3 {0} -l {1} -o {2}'.format(SCRIPT_DIR,infile,outfile)
        else:
            earlier_infile = os.path.join(args.base_in_dir,get_earlier_filename(filename))
            out = out + 'python3 {0} -l {1} -o {2} -e {3}'.format(SCRIPT_DIR,infile,outfile,earlier_infile)
        out = out + '\n'
    print(out)
    f.write(out)

def main(args):
    utils.log('found {0} files in {1}'.format(len(os.listdir(args.base_in_dir)),args.base_in_dir))
    for i,filename in enumerate(os.listdir(args.base_in_dir)):
        infile = os.path.join(args.base_in_dir, filename)
        outfile = os.path.join(args.base_out_dir, filename)
        later = pd.read_csv(infile)
        num = int(strip_filename(filename))
        if num == 0:
            utils.log('writing {0}'.format(outfile))
            later.to_csv(outfile)
        else:
            earlier_infile = os.path.join(args.base_in_dir,get_earlier_filename(filename))
            utils.log('subtracting [{0} - {1}]'.format(infile, earlier_infile))
            earlier = pd.read_csv(earlier_infile)
            result = subtract_dfs(earlier=earlier,
                                  later=later)
            utils.log('writing {0}'.format(outfile))
            result.to_csv(outfile)
    utils.log('wrote {0} files to {1}'.format(i,args.base_out_dir))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='process wiki data')
    parser.add_argument('-e', '--earlier',
                        help='path to the earlier csv (to subtract)')
    parser.add_argument('-l', '--later',
                        help='path to the later csv (to subtract from)')
    parser.add_argument('-o', '--outfile',
                        help='path to directory for output files')
    parser.add_argument('-j', '--job_script',
                        help='create a job script at the specified file path for use on hyak')
    parser.add_argument('--base_in_dir',
                        help='base input directory (use with --job_script)')
    parser.add_argument('--base_out_dir',
                        help='base output directory (use with --job_script)')
    parser.add_argument('--iterative',
                        help='perform iteratively on a single processor')
    args = parser.parse_args()
    if args.job_script:
        job_script(args)
    elif args.iterative:
        main(args)
    elif args.earlier:
        later = pd.read_csv(args.later)
        earlier = pd.read_csv(args.earlier)
        df = subtract_dfs(earlier=earlier,later=later)
        df.to_csv(args.outfile,na_rep='NaN',encoding='utf-8',index=False)
    else:
        pd.read_csv(args.later).to_csv(args.outfile,na_rep='NaN',encoding='utf-8',index=False)
