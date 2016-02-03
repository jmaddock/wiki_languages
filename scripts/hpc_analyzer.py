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
        statistics.append('total')
        f = basic.write_to_results('/edits_%s.csv' % self.lang)
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
        print(df)
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
        for lang in self.langs:
            pages = self.db[lang].find({'rev_len.no_revert':{'$gt':1}}).limit(10)
            total = pages.count()
            df = DataFrame({'len':[],'revert':[],'namespace':[]})
            for i,p in enumerate(pages):
                for r in self.revert:
                    df = df.append(DataFrame({
                        'len':[p['rev_len'][r]],
                        'revert':[revert.index(r)],
                        'namespace':[p['namespace']]
                    }))
                if i % 1000 == 0 and i != 0 and v:
                    basic.log('processed %s/%s documents' % (i,total))
            print(df)
            for n in self.namespace:
                for r in self.revert:
                    basic.log('%s %s %s' % (lang,n,r))
                    result = df.loc[(df['namespace'] == namespace.index(n)) & (df['revert'] == revert.index(r)),'len'].value_counts()
                    result.to_csv(basic.results_path('/distributions/%s_%s_%s.csv' % (lang,n,r)),encoding='utf-8')
                    print(result)
            
def main():
    parser = argparse.ArgumentParser(description='process wiki dumps')
    parser.add_argument('lang')
    args = parser.parse_args()
    a = Analyzer(args.lang)    
    a.edit_statistics(statistics=['total','var','std','mean','median'])
    #a.edit_histogram()

if __name__ == "__main__":
    main()
