import pymongo
import basic
from collections import Counter,defaultdict
from pandas import DataFrame

class Analyzer(object):
    def __init__(self,langs,namespace=['a','t','at'],revert=['revert','no_revert']):
        self.client = pymongo.MongoClient()
        self.db = self.client['edit_history']
        self.langs = langs
        self.namespace = namespace
        self.revert = revert

    ## DEPRECIATED
    def total_talk_vs_article(self,revert=True):
        f = basic.write_to_results('total_talk_vs_article.csv')
        header = '"lang","total","article","talk"\n'
        f.write(header)
        result = {}
        for lang in self.langs:
            if revert:
                a = self.db[lang].find({'namespace':0,'rev_len.revert':{'$gt':1}}).count()
                t = self.db[lang].find({'namespace':1,'rev_len.revert':{'$gt':1}}).count()
            else:
                a = self.db[lang].find({'namespace':0,'rev_len.no_revert':{'$gt':1}}).count()
                t = self.db[lang].find({'namespace':1,'rev_len.no_revert':{'$gt':1}}).count()
            total = a+t
            out = '"%s",%s,%s,%s\n' % (lang,total,a,t)
            f.write(out)
            result[lang] = {
                'total':total,
                'a':a,
                't':t
            }
        f.close()
        return result

    ## DEPRECIATED
    def mean_talk_vs_article_edits(self,revert=True):
        f = basic.write_to_results('mean_talk_vs_article_edits.csv')
        header = '"lang","a+t_total","a+t_average ","article_total","article_average","talk_total","talk_average"\n'
        f.write(header)
        result = {}
        for lang in self.langs:
            c = Counter()
            if revert:
                pages = self.db[lang].find({'rev_len.revert':{'$gt':1}})
            else:
                pages = self.db[lang].find({'rev_len.no_revert':{'$gt':1}})
            for p in pages:
                if len(p['rev']) > 1:
                    if p['namespace'] == 1:
                        if revert:
                            c.update({'talk':p['rev_len']['revert'],'t_count':1})
                        else:
                            c.update({'talk':p['rev_len']['no_revert'],'t_count':1})
                    else:
                        if revert:
                            c.update({'article':p['rev_len']['revert'],'a_count':1})
                        else:
                            c.update({'article':p['rev_len']['no_revert'],'a_count':1})
            at_count = c['a_count']+c['t_count']
            at = c['article'] + c['talk']
            at_mean = float(at)/at_count
            a_mean = float(c['article'])/c['a_count']
            t_mean = float(c['talk'])/c['t_count']
            out = '"%s",%s,%s,%s,%s,%s,%s\n' % (lang,
                                                at,
                                                at_mean,
                                                c['article'],
                                                a_mean,
                                                c['talk'],
                                                t_mean)
            f.write(out)
            result[lang] = {
                'at_total':at,
                'at_mean':at_mean,
                'a_total':c['article'],
                'a_mean':a_mean,
                't_total':c['talk'],
                't_mean':t_mean
            }
        f.close()
        return result

    ## DEPRECIATED
    def median_talk_vs_article_edits(self,revert=True):
        f = basic.write_to_results('median_talk_vs_article_edits.csv')
        header = '"lang","a+t_total","a+t_median","article_total","article_median","talk_total","talk_median"\n'
        f.write(header)
        stats = {}
        result = {}
        for lang in self.langs:
            c = Counter()
            if revert:
                stats['a'] = {'db':self.db[lang].find({'namespace':0,'rev_len.revert':{'$gt':1}}).sort('rev_len.revert',1)}
                stats['t'] = {'db':self.db[lang].find({'namespace':1,'rev_len.revert':{'$gt':1}}).sort('rev_len.revert',1)}
                stats['at'] = {'db':self.db[lang].find({'rev_len.revert':{'$gt':1}}).sort('rev_len.revert',1)}
            else:
                stats['a'] = {'db':self.db[lang].find({'namespace':0,'rev_len.no_revert':{'$gt':1}}).sort('rev_len.no_revert',1)}
                stats['t'] = {'db':self.db[lang].find({'namespace':1,'rev_len.no_revert':{'$gt':1}}).sort('rev_len.no_revert',1)}
                stats['at'] = {'db':self.db[lang].find({'rev_len.no_revert':{'$gt':1}}).sort('rev_len.no_revert',1)}
            for x in stats:
                stats[x]['count'] = stats[x][db].count()
                for i,p in enumerate(stats[x][db]):
                    if i == int(stats[x]['count']/2):
                        if stats[x]['count'] % 2 == 0:
                            if revert:
                                stats[x]['median'] = p['rev_len']['revert']
                            else:
                                stats[x]['median'] = p['rev_len']['no_revert']
                            break
                        else:
                            if revert:
                                median1 = p['rev_len']['revert']
                                median2 = p.next()['rev_len']['revert']
                            else:
                                median1 = p['rev_len']['no_revert']
                                median2 = p.next()['rev_len']['no_revert']
                            stats[x]['median'] = (median1 + median2)/2
                            break
            out = '"%s",%s,%s,%s,%s,%s,%s\n' % (lang,
                                                stats['at']['count'],
                                                stats['at']['median'],
                                                stats['a']['count'],
                                                stats['a']['median'],
                                                stats['t']['count'],
                                                stats['t']['median'])
            f.write(out)
            result[lang] = {
                'at_total':stats['at']['count'],
                'at_median':stats['at']['median'],
                'a_total':stats['a']['count'],
                'a_median':stats['a']['mean'],
                't_total':stats['t']['count'],
                't_median':stats['t']['median'],
            }
        f.close()
        return result

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
