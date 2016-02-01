import basic
from collections import Counter,defaultdict
from pandas import DataFrame

class Analyzer(object):
    def __init__(self,langs,namespace=['a','t','at'],revert=['revert','no_revert']):
        self.langs = langs
        self.namespace = namespace
        self.revert = revert
        self.db_path = os.path.join(os.path.dirname(__file__),os.pardir,'db/')
    
    def edit_statistics(self,statistics,v=False):
        statistics.append('total')
        f = basic.write_to_results('median_talk_vs_article_edits.csv')
        header = '"lang"'
        for n in self.namespace:
            for r in self.revert:
                for s in statistics:    
                    header = header + ((',"%s_%s_%s"') % (n,s,r))
        header = header + '\n'
        f.write(header)
        result = defaultdict(dict)
        for lang in self.langs:
            f.write('"%s",' % lang)
            result[lang] = defaultdict(dict)
            pages = self.db[lang].find({'rev_len.no_revert':{'$gt':1}})
            total = pages.count()
            df = DataFrame({'len':[],'revert':[],'namespace':[]})
            for i,p in enumerate(pages):
                for r in self.revert:
                    df = df.append(DataFrame({
                        'len':[p['rev_len'][r]],
                        'revert':[self.revert.index(r)],
                        'namespace':[p['namespace']]
                    }))
                if i % 1000 == 0 and i != 0 and v:
                    basic.log('processed %s/%s documents' % (i,total))
            print(df)
            for n in self.namespace:
                result[lang][n] = defaultdict(dict)
                for r in self.revert:
                    result[lang][n][r] = defaultdict(dict)
                    basic.log('%s %s %s' % (lang,n,r))
                    for s in statistics:
                        if s == 'total':
                            result[lang][n][r][s] = df.loc[(df['namespace'] == namespace.index(n)) & (df['revert'] == revert.index(r)),'len'].sum()
                        elif s == 'var':
                            result[lang][n][r][s] = df.loc[(df['namespace'] == namespace.index(n)) & (df['revert'] == revert.index(r)),'len'].var()
                        elif s == 'std':
                            result[lang][n][r][s] = df.loc[(df['namespace'] == namespace.index(n)) & (df['revert'] == revert.index(r)),'len'].std()
                        elif s == 'mean':
                            result[lang][n][r][s] = df.loc[(df['namespace'] == namespace.index(n)) & (df['revert'] == revert.index(r)),'len'].mean()
                        elif s == 'median':
                            result[lang][n][r][s] = df.loc[(df['namespace'] == namespace.index(n)) & (df['revert'] == revert.index(r)),'len'].median()
                        f.write(',%s' % result[lang][n][r][s])
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
    langs = ['simple']
    a = Analyzer(langs)
    #a.total_talk_vs_article()
    #a.edit_statistics(statistics=['total','var','std','mean','median'])
    a.edit_histogram()

if __name__ == "__main__":
    main()
