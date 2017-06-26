import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
import utils
import argparse

def subtract_dfs(earlier,later):
    earlier = earlier.set_index(['page_id_1', 'lang'])
    later = later.set_index(['page_id_1', 'lang'])
    result = later[['len_1', 'len_0', 'no_revert_len_1', 'no_revert_len_0', ]].subtract(
        earlier[['len_1', 'len_0', 'no_revert_len_1', 'no_revert_len_0']])
    result = result.join(later[['title', 'tds_1', 'tds_0', 'num_editors_1', 'num_editors_0', 'page_id_0']])
    return result

def strip_filename(filename):
    return filename.split('_')[-1].replace('.csv','')

def get_earlier_filename(filename):
    return '{0}_{1}.csv'.format(filename.split('_')[-2],int(strip_filename(filename))-1)

def main(args):
    utils.log('found {0} files in {1}'.format(len(os.listdir(args.indir)),args.indir))
    for i,filename in enumerate(os.listdir(args.indir)):
        infile = os.path.join(args.indir, filename)
        outfile = os.path.join(args.outdir, filename)
        later = pd.read_csv(infile)
        num = int(strip_filename(filename))
        if num == 0:
            utils.log('writing {0}'.format(outfile))
            later.to_csv(outfile)
        else:
            earlier_infile = os.path.join(args.indir,get_earlier_filename(filename))
            utils.log('subtracting [{0} - {1}]'.format(infile, earlier_infile))
            earlier = pd.read_csv(earlier_infile)
            result = subtract_dfs(earlier=earlier,
                                  later=later)
            utils.log('writing {0}'.format(outfile))
            result.to_csv(outfile)
    utils.log('wrote {0} files to {1}'.format(i,args.outdir))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='process wiki data')
    parser.add_argument('-i', '--indir',
                        help='path to directory containing combined (all language editions) edit counts thresholded by relative date')
    parser.add_argument('-o', '--outdir',
                        help='path to directory for output files')
    args = parser.parse_args()
    main(args)