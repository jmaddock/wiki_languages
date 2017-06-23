import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
import utils
import argparse
import subprocess
from concurrent.futures import ProcessPoolExecutor
from functools import partial
import re

class ThreadedModelHandler(object):
        def __init__(self,indir,outdir,stata_do_file,num_workers=4,debug_file=None):
            self.indir = indir
            self.outdir = outdir
            self.num_workers = num_workers
            self.stata_do_file = stata_do_file
            self.infile_list = []
            self.process_pool = []
            self.debug_file = debug_file

        def get_file_lists(self):
            if not self.debug_file:
                self.infile_list = [os.path.join(self.indir,x) for x in os.listdir(self.indir) if x[-4:] == '.csv']
                pattern = re.compile('([0-9]+).csv')
                self.outfile_list = [os.path.join(self.outdir,pattern.search(x).group(1)) for x in self.infile_list]
            else:
                self.infile_list = [self.debug_file for  i in range(self.num_workers)]
                self.outfile_list = [os.path.join(self.outdir,'model_debug_{0}'.format(i)) for i in range(self.num_workers)]

        def run_model_subprocess(self,infile,outfile):
            utils.log('processing {0}'.format(infile))
            subprocess.run(['stata','-e','do',self.stata_do_file,infile,outfile])
            utils.log('finished {0}'.format(outfile))

        def run_models(self):
            with ProcessPoolExecutor(max_workers=self.num_workers) as executor:
                executor.map(self.run_model_subprocess,
                             self.infile_list,
                             self.outfile_list)
            utils.log('processed {0} files'.format(len(self.infile_list)))
            
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='process wiki data')
    parser.add_argument('-i','--indir',
                        help='input directory path')
    parser.add_argument('-o','--outdir',
                        help='ouput directory path')
    parser.add_argument('-s','--stata_do_file',
                        help='file path for stata .do file to calculate models')
    parser.add_argument('-w','--workers',
                        type=int,
                        help='the number of workers to use for processing')
    parser.add_argument('-d','--debug',
                        help='use the same dta file for all processes to test output')
    args = parser.parse_args()
    utils.log('INPUT DIR: {0}\nOUTPUT DIR: {1}'.format(args.indir,args.outdir))
    threaded_model_handler = ThreadedModelHandler(indir=args.indir,
                                                  outdir=args.outdir,
                                                  stata_do_file=args.stata_do_file,
                                                  num_workers=args.workers,
                                                  debug_file=args.debug)
    threaded_model_handler.get_file_lists()
    utils.log('WORKERS: {0}\nTOTAL FILES: {1}\nDEBUG: {2}'.format(args.workers,len(threaded_model_handler.infile_list),args.debug))
    threaded_model_handler.run_models()
