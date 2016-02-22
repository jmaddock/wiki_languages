import basic
import argparse
import os
import pandas as pd
import numpy as np
from collections import Counter,defaultdict
from pandas import DataFrame

class Analyzer(object):
    def __init__(self,lang,namespace=['a','t','at'],revert=['len','no_revert_len'],drop1=False):
        self.lang = lang
        self.namespace = namespace
        self.revert = revert
        self.db_path = os.path.join(os.path.dirname(__file__),os.pardir,'db/%s/linked_edit_counts.csv' % (lang))
        self.drop1 = drop1
    
    def edit_statistics(self,statistics,v=False):
        f_out = basic.create_dir('results/basic_stats')
        if self.drop1:
            f = open('%s/edits_drop1_%s.csv' % (f_out,self.lang),'w')
        else:
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
        if self.drop1:
            df = df.loc[(df['len'] > 1)]
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
                        if self.namespace.index(n) == (len(self.namespace)-1):
                            result[self.lang][n][r][s] = float(result[self.lang]['a'][r]['mean'])/result[self.lang]['t'][r]['mean']
                    elif s == 'missing_talk':
                        if self.namespace.index(n) == (len(self.namespace)-1):
                            result[self.lang][n][r][s] = df.loc[(df['linked_id'] == 'NONE')]
                    
                    f.write(',%s' % result[self.lang][n][r][s])
        f.write('\n')
        f.close()
        return result

    
    def edit_histogram(self,plot=True,v=False):
        basic.log('creating edit histogram %s' % self.lang)
        f_out = basic.create_dir('results/histograms')
        df = pd.read_csv(self.db_path)
        if self.drop1:
            df = df.loc[(df['len'] > 1)]
        for n in self.namespace:
            for r in self.revert:
                basic.log('%s %s %s' % (self.lang,n,r))
                if n == 'at':
                    result = df[r].value_counts()
                else:
                    result = df.loc[(df['namespace'] == self.namespace.index(n)),r].value_counts()
                result.columns = ['articles']
                result.to_csv('%s/%s_%s_%s.csv' % (f_out,self.lang,n,r),encoding='utf-8',index_label='edits')

    def edit_quantiles(self,q=.01,quantile_range=False,v=False):
        basic.log('creating edit quantiles %s' % self.lang)
        f_out = basic.create_dir('results/quantiles')
        df = pd.read_csv(self.db_path)
        df.page_id = df.page_id.astype(int)
        if self.drop1:
            df = df.loc[(df['len'] > 1)]
        q = np.arange(q,1+q,q)
        for n in self.namespace:
            for r in self.revert:
                basic.log('%s %s %s' % (self.lang,n,r))
                if n == 'at':
                    result = df[r].quantile(q=q)
                    mean = df[r].mean()
                else:
                    result = df.loc[(df['namespace'] == self.namespace.index(n)),r].quantile(q=q)
                    #qcut = pd.qcut(df.loc[(df['namespace'] == self.namespace.index(n)),r],q)
                    #print(qcut)
                    mean = df.loc[(df['namespace'] == self.namespace.index(n)),r].mean()
                result = result.to_frame()
                column = '%s_%s_%s' % (self.lang,n,r)
                result.columns = [column]
                result = result.append(DataFrame({column:mean},index=['mean_value']))
                result = result.append(DataFrame({column:result.loc[(result[column] == int(mean))].tail(1).index.values},index=['mean_quantile']))
                #result = result.append(DataFrame({column:}))
                result.to_csv('%s/%s_%s_%s.csv' % (f_out,self.lang,n,r),encoding='utf-8',index_label='qauntiles')

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

    def edit_ratio_histogram(self):
        basic.log('creating edit histogram %s' % self.lang)
        f_out = basic.create_dir('results/ratio_histograms')
        df = pd.read_csv(self.db_path)
        df.page_id = df.page_id.astype(float)
        df = df.loc[df['linked_id'] != 'NONE']
        if self.drop1:
            df = df.loc[(df['len'] > 1)]
        for r in self.revert:
            basic.log('%s %s' % (self.lang,r))
            n0 = df.loc[(df['namespace'] == 0)].set_index('page_id',drop=False)
            n1 = df.loc[(df['namespace'] == 1)].set_index('linked_id',drop=False)
            ratio = n0[r].divide(n1[r],axis='index').to_frame()
            ratio.columns = ['ratio']
            ratio = n0.join(ratio).set_index('page_id')
            result = ratio['ratio'].value_counts()
            result.columns = ['pages']
            result.to_csv('%s/%s_%s.csv' % (f_out,self.lang,r),encoding='utf-8',index_label='edit_ratio')

def job_script(args):
    f = open(args.job_script,'w')
    script_dir = os.path.abspath(__file__)
    lang_dir = os.path.join(os.path.dirname(__file__),os.pardir,'db/')
    langs = [name for name in os.listdir(lang_dir) if os.path.isdir(lang_dir+name)]
    for a in args.analysis:
        for l in langs:
            out = 'python3 %s -l %s -a %s' % (script_dir,l,a)
            if args.namespace:
                out = '%s -n' (out)
                for n in args.namespace:
                    out = '%s %s' % (out,n)
            if args.revert:
                out = '%s --revert' % out
            if args.no_revert:
                out = '%s --no_revert' % out
            if args.drop1:
                out = '%s --drop1' % out
            print(out)
            #f.write(out+'\n')

def main():
    parser = argparse.ArgumentParser(description='process wiki dumps')
    parser.add_argument('-l','--lang')
    parser.add_argument('-a','--analysis',nargs='+')
    parser.add_argument('-n','--namespace',nargs='*')
    parser.add_argument('--revert',action='store_true')
    parser.add_argument('--no_revert',action='store_true')
    parser.add_argument('--f_out')
    parser.add_argument('-j','--job_script')
    parser.add_argument('-v','--verbose',action='store_true')
    parser.add_argument('--drop1',action='store_true')
    args = parser.parse_args()
    if args.job_script:
        job_script(args)
    else: 
        revert = []
        if args.revert:
            revert.append('len')
        if args.no_revert:
            revert.append('no_revert_len')
        a = Analyzer(args.lang,args.namespace,revert,args.drop1)
        if 'stats' in args.analysis:
            a.edit_statistics(statistics=['total','var','std','mean','median','total_ratio','mean_ratio','missing_talk'])
        if 'dist' in args.analysis:
            a.edit_histogram()
        if 'quant' in args.analysis:
            a.edit_quantiles()
        if 'combine_quant' in args.analysis:
            a.combine_quantiles()
        if 'erh' in args.analysis:
            a.edit_ratio_histogram()

if __name__ == "__main__":
    main()
