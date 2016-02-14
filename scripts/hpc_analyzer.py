import basic
import argparse
import os
import pandas as pd
import numpy as np
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
        f = open('%s/edits_%s.csv' % (f_out,self.lang),'w')
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
                    elif s == 'total_ratio':
                        if n == 't':
                            result[self.lang][n][r][s] = float(result[self.lang]['a'][r]['total'])/result[self.lang]['t'][r]['total']
                    elif s == 'mean_ratio':
                        print(self.namespace.index(n))
                        print(len(self.namespace)-1)
                        if self.namespace.index(n) == (len(self.namespace)-1):
                            result[self.lang][n][r][s] = float(result[self.lang]['a'][r]['mean'])/result[self.lang]['t'][r]['mean']
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

    def edit_quantiles(self,v=False):
        f_out = basic.create_dir('results/quantiles')
        df = pd.read_csv(self.db_path)
        q = np.arange(0,1,.10)
        for n in self.namespace:
            for r in self.revert:
                basic.log('%s %s %s' % (self.lang,n,r))
                if n == 'at':
                    result = df[r].quantile(q=q)
                else:
                    result = df.loc[(df['namespace'] == self.namespace.index(n)),r].quantile(q=q)
                print(result)
                result.to_csv('%s/%s_%s_%s.csv' % (f_out,self.lang,n,r),encoding='utf-8')

    def combine_quantiles(self):
        data_dir = os.path.join(os.path.dirname(__file__),os.pardir,'results/quantiles/')
        result = None
        for i,f in enumerate(os.listdir(data_dir)):
            if f == 'combined.csv':
                continue
            elif i == 0:
                result = pd.read_csv(data_dir+f)
            else:
                df = pd.read_csv(data_dir+f)
                result.join(df)
        result.to_csv('%s/combined.csv' % (data_dir),encoding='utf-8')

def job_script(args):
    f = open(args.job_script,'w')
    script_dir = os.path.abspath(__file__)
    lang_dir = os.path.join(os.path.dirname(__file__),os.pardir,'db/')
    langs = [name for name in os.listdir(lang_dir) if os.path.isdir(lang_dir+name)]
    print(os.listdir(lang_dir))
    for a in args.analysis:
        for l in langs:
            out = 'python3 %s -l %s -a %s -n' % (script_dir,l,a)
            for n in args.namespace:
                out = '%s %s' % (out,n)
            if args.revert:
                out = '%s --revert' % out
            if args.no_revert:
                out = '%s --no_revert' % out
            print(out)
            f.write(out)

def main():
    parser = argparse.ArgumentParser(description='process wiki dumps')
    parser.add_argument('-l','--lang')
    parser.add_argument('-a','--analysis',nargs='+')
    parser.add_argument('-n','--namespace',nargs='+')
    parser.add_argument('--revert',action='store_true')
    parser.add_argument('--no_revert',action='store_true')
    parser.add_argument('--f_out')
    parser.add_argument('-j','--job_script')
    parser.add_argument('-v','--verbose',action='store_true')
    args = parser.parse_args()
    if args.job_script:
        job_script(args)
    else:
        print(args)
        revert = []
        if args.revert:
            revert.append('len')
        if args.no_revert:
            revert.append('no_revert_len')
        a = Analyzer(args.lang,args.namespace,revert)
        if 'stats' in args.analysis:
            a.edit_statistics(statistics=['total','var','std','mean','median','total_ratio','mean_ratio'])
        if 'dist' in args.analysis:
            a.edit_histogram()
        if 'quant' in args.analysis:
            a.edit_quantiles()
        if 'combine_quant' in args.analysis:
            a.combine_quantiles()

if __name__ == "__main__":
    main()
