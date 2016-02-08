import basic
import argparse
import os
import pandas as pd
from collections import Counter,defaultdict
from pandas import DataFrame

class Analyzer(object):
    def __init__(self,lang,namespace=['a','t','at'],revert=['len','no_revert_len']):
        self.lang = lang
        self.namespace = namespace
        self.revert = revert
        self.db_path = os.path.join(os.path.dirname(__file__),os.pardir,'db/%s/linked_edit_counts.csv' % (lang))
    
    def edit_statistics(self,statistics,v=False):
        f_out = basic.create_dir('results/basic_stats')
        f = basic.write_to_results('%s/edits_%s.csv' % (f_out,self.lang))
        header = '"lang"'
        for n in self.namespace:
            for r in self.revert:
                for s in statistics:    
                    header = header + ((',"%s_%s_%s"') % (n,s,r))
        header = header + '\n'
        f.write(header)
        result = defaultdict(dict)
        f.write('"%s"' % self.lang)
        result[self.lang] = defaultdict(dict)
        df = pd.read_csv(self.db_path)
        for n in self.namespace:
            result[self.lang][n] = defaultdict(dict)
            for r in self.revert:
                result[self.lang][n][r] = defaultdict(dict)
                basic.log('%s %s %s' % (self.lang,n,r))
                for s in statistics:
                    if s == 'total':
                        if n == 'at':
                            result[self.lang][n][r][s] = df[r].sum()
                        else:
                            result[self.lang][n][r][s] = df.loc[(df['namespace'] == self.namespace.index(n)),r].sum()
                    elif s == 'var':
                        if n == 'at':
                            result[self.lang][n][r][s] = df[r].var()
                        else:
                            result[self.lang][n][r][s] = df.loc[(df['namespace'] == self.namespace.index(n)),r].var()
                    elif s == 'std':
                        if n == 'at':
                            result[self.lang][n][r][s] = df[r].std()
                        else:
                            result[self.lang][n][r][s] = df.loc[(df['namespace'] == self.namespace.index(n)),r].std()
                    elif s == 'mean':
                        if n == 'at':
                            result[self.lang][n][r][s] = df[r].mean()
                        else:
                            result[self.lang][n][r][s] = df.loc[(df['namespace'] == self.namespace.index(n)),r].mean()
                    elif s == 'median':
                        if n == 'at':
                            result[self.lang][n][r][s] = df[r].median()
                        else:
                            result[self.lang][n][r][s] = df.loc[(df['namespace'] == self.namespace.index(n)),r].median()
                    f.write(',%s' % result[self.lang][n][r][s])
        f.write('\n')
        f.close()
        print(result)
        return result

    
    def edit_histogram(self,plot=True,v=False):
        f_out = basic.create_dir('results/histograms')
        df = pd.read_csv(self.db_path)
        for n in self.namespace:
            for r in self.revert:
                basic.log('%s %s %s' % (self.lang,n,r))
                if n == 'at':
                    result = df[r].value_counts()
                else:
                    result = df.loc[(df['namespace'] == self.namespace.index(n)),r].value_counts()
                result.to_csv('%s/%s_%s_%s.csv' % (f_out,self.lang,n,r),encoding='utf-8')

def job_script(f_out,analysis):
    f = open(f_out,'w')
    script_dir = os.path.dirname(__file__)
    lang_dir = os.path.join(os.path.dirname(__file__),os.pardir,'db/')
    langs = [name for name in os.listdir(dirs) if os.path.isdir(lang_dir+name)]
    print(os.listdir(dirs))
    for a in analysis:
        for l in langs:
            out = 'python3 %s/hpc_analyzer.py -l %s -a %s\n' % (script_dir,l,a)
            print(out)
            f.write(out)

def main():
    parser = argparse.ArgumentParser(description='process wiki dumps')
    parser.add_argument('-l','--lang')
    parser.add_argument('-a','--analysis',nargs='+')
    parser.add_argument('--f_out')
    parser.add_argument('-j','--job_script',action='store_true')
    args = parser.parse_args()
    if args.job_script:
        job_script(args.f_out,args.analysis)
    else:
        a = Analyzer(args.lang)
        if 'stats' in args.analysis:
            a.edit_statistics(statistics=['total','var','std','mean','median'])
        if 'dist' in args.analysis:
            a.edit_histogram()

if __name__ == "__main__":
    main()
