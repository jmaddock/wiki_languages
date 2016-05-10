from mw import xml_dump,Timestamp
from collections import Counter
import single_dump_handler
import pandas as pd
import os
import datetime
import basic
import json
import argparse
import config

## TO START MONGOD INSTANCE ON OMNI:
## mongod --dbpath ~/jim/wiki_data/mongodb_data/ --fork --logpath ~/jim/wiki_data/mongodb_data/logs/mongodb.log

class Page_Edit_Counter(object):
    def __init__(self,wiki_name,db_path=None,namespace=['a','t','at'],revert=['len','no_revert_len'],drop1=False):
        basic.log('creating importer...')
        self.namespace = namespace
        self.revert = revert
        self.wiki_name = wiki_name
        if not db_path:
            self.db_path = os.path.join(os.path.dirname(__file__),os.pardir,'db/') + wiki_name
        else:
            self.db_path = db_path
        self.create_db_dir()
        self.drop1 = drop1

    def create_db_dir(self):
        if not os.path.exists(self.db_path):
            print('creating dir: %s' % self.db_path)
            os.makedirs(self.db_path)

    def link_documents(self,f_in=None,v=False):
        basic.log('linking %s' % self.wiki_name)
        if not isinstance(f_in, pd.DataFrame):
            f_in_name = '%s/edit_counts.csv' % self.db_path
            basic.log('loading data from file %s' % f_in_name)
            f_in = pd.read_csv(f_in_name)
        if self.drop1:
            f_in = f_in.loc[(f_in['len'] > 1)]
        f_in.title = f_in.title.astype(str)
        n0 = f_in.loc[(f_in['namespace'] == 0)].set_index('title',drop=False)
        n1 = f_in.loc[(f_in['namespace'] == 1)].set_index('title',drop=False)
        l0 = f_in.loc[(f_in['namespace'] == 0)].set_index('title',drop=False)[['page_id']]
        l1 = f_in.loc[(f_in['namespace'] == 1)].set_index('title',drop=False)[['page_id']]
        l0.rename(columns = {'page_id':'linked_id'}, inplace = True)
        l1.rename(columns = {'page_id':'linked_id'}, inplace = True)
        result0 = n0.join(l1).set_index('page_id',drop=False)
        result1 = n1.join(l0).set_index('page_id',drop=False)
        result = result0.append(result1)
        if v:
            print(result)
        result['page_id'] = result.index
        result_path = '%s/linked_edit_counts.csv' % (self.db_path)
        columns = ['page_id','title','namespace','len','no_revert_len','num_editors','td','tds','lang','linked_id']
        result = self.drop_dups(result)
        result.to_csv(result_path,na_rep='NaN',columns=columns,encoding='utf-8')
        return result

    ## drop of df duplicates based on page_id field
    ## log number of duplicates dropped
    def drop_dups(self,df):
        num_dups = len(df.set_index('page_id',drop=False).index.get_duplicates())
        percent = num_dups/len(df)
        basic.log('dropped %s (%.2f%%) duplicates' % (num_dups,percent))
        return df.drop_duplicates(subset='page_id',keep=False)
    
    ## reduce edit csv to page level csv counting edits
    ## INCLUDES TIMEDELTA AND NUM_EDITORS
    def rev_size(self,v=False):
        basic.log('creating %s edit counts' % self.wiki_name)
        f_in_name = '%s/%s' % (self.db_path,config.COMBINED_RAW_EDITS)
        basic.log('loading data from file %s' % f_in_name)
        f_in = pd.read_csv(f_in_name,escapechar='//')
        nr = f_in.loc[(f_in['revert'] == False)]
        df = f_in[['page_id','title','namespace']].drop_duplicates(subset='page_id').set_index('page_id',drop=False)
        s = f_in['page_id'].value_counts().to_frame('len')
        nrs = nr['page_id'].value_counts().to_frame('no_revert_len')
        result = df.join(s).join(nrs)
        result_path = '%s/edit_counts.csv' % (self.db_path)
        columns = ['page_id','title','namespace','len','no_revert_len','num_editors','td','tds','lang']
        age = self.page_age(f_in)
        editors = self.num_editors(f_in)
        result = result.merge(age,on='page_id').merge(editors,on='page_id')
        result['lang'] = self.wiki_name
        if self.drop1:
            result = result.loc[(result['len'] > 1)]
        result.to_csv(result_path,na_rep='NaN',columns=columns,encoding='utf-8')
        if v:
            print(result)
        return result

    ## find the timedelta between the first and the last edit to a page (not between edits)
    ## return reduced dataframe
    def page_age(self,df):
        basic.log('getting %s time deltas' % self.wiki_name)
        df['ts'] = pd.to_datetime(df['ts'],format="%Y-%m-%d %H:%M:%S")
        tdf = df.set_index('ts',drop=False).sort_index(ascending=True)
        first = tdf[['page_id','namespace','ts']].drop_duplicates(subset='page_id',keep='first')
        first = first.set_index('page_id',drop=False)
        last = tdf[['page_id','namespace','ts']].drop_duplicates(subset='page_id',keep='last')
        last = last.set_index('page_id',drop=False)
        result = last['ts'].subtract(first['ts'],axis='index',fill_value=-1).to_frame()
        result['tds'] = result['ts'].astype('timedelta64[s]')
        result = result.rename(columns = {'ts':'td'})
        result['page_id'] = result.index
        return result

    ## get the number of unique editors who have contributed to a group of articles
    ## return reduced dataframe
    def num_editors(self,df):
        basic.log('creating %s author counts' % self.wiki_name)
        df = df.groupby('page_id').user_text.nunique().to_frame()
        df.columns = ['num_editors']
        df['page_id'] = df.index
        return df

    def append_article_to_talk(self,df=None,write=True):
        basic.log('merging %s namespaces into single dataframe' % self.wiki_name)
        if not isinstance(df, pd.DataFrame):
            f_in_name = '%s/linked_edit_counts.csv' % self.db_path
            basic.log('loading data from file %s' % f_in_name)
            df = pd.read_csv(f_in_name)
        n0 = df.loc[(df['namespace'] == 0) & (df['linked_id'] != None)]
        n0 = n0.rename(columns = {'linked_id':'to_merge'})
        n0.to_merge = n0.to_merge.astype(float)
        n1 = df.loc[(df['namespace'] == 1)]
        n1 = n1.rename(columns = {'page_id':'to_merge'})
        #print(n1)
        #print(n0.loc[n0['editor_ratio'] == None])
        n1.to_merge = n1.to_merge.astype(float)
        merged = n1.merge(n0,on='to_merge',how='inner',suffixes=['_1','_0'])
        result_path = '%s/merged_edit_counts.csv' % (self.db_path)
        #print(merged.columns.values)
        merged = merged.rename(columns = {'to_merge':'page_id_1',
                                          'linked_id':'page_id_0',
                                          'title_1':'title',
                                          'lang_1':'lang'})
        columns = ['page_id_1','title','len_1','no_revert_len_1','num_editors_1','td_1','tds_1','lang','page_id_0','len_0','no_revert_len_0','num_editors_0','td_0','tds_0']
        if 'ratio_0' in merged.columns.values:
            merged = merged.rename(columns = {'ratio_0':'ratio','editor_ratio_0':'editor_ratio'})
            columns = columns + ['ratio','editor_ratio']
        if write:
            merged.to_csv(result_path,na_rep='NaN',columns=columns,encoding='utf-8')
        return merged

    def edit_ratios(self,df=None,r='len'):
        basic.log('creating edit ratios %s' % self.wiki_name)
        if not isinstance(df, pd.DataFrame):
            f_in_name = '%s/linked_edit_counts.csv' % self.db_path
            basic.log('loading data from file %s' % f_in_name)
            df = pd.read_csv(f_in_name)
        df.page_id = df.page_id.astype(float)
        df = df.loc[df['linked_id'] != None]
        df.linked_id = df.linked_id.astype(float)
        df = self.drop_dups(df)
        basic.log('dropped %s duplicates' % len(df.set_index('page_id',drop=False).index.get_duplicates()))
        df = df.drop_duplicates(subset='page_id',keep=False)
        if self.drop1:
            df = df.loc[(df['len'] > 1)]
        basic.log('%s' % (self.wiki_name))
        basic.log('%s pages' % len(df))
        n0 = df.loc[(df['namespace'] == 0) & (df['linked_id'] != None)].set_index('page_id',drop=False)
        n1 = df.loc[(df['namespace'] == 1) & (df['linked_id'] != None)].set_index('linked_id',drop=False)
        basic.log('%s articles' % len(n0))
        basic.log('%s talk' % len(n1))
        ratio = n0[r].divide(n1[r],axis='index',fill_value=-1).to_frame()
        ratio.columns = ['ratio']
        ratio.ratio = ratio.ratio.astype(int)
        editor_ratio = n0['num_editors'].divide(n1['num_editors'],axis='index',fill_value=-1).to_frame()
        editor_ratio.columns = ['editor_ratio']
        editor_ratio.editor_ratio = editor_ratio.editor_ratio.astype(int)
        #print(editor_ratio)
        ratio = n0.join(ratio).join(editor_ratio).set_index('page_id')
        #print(ratio)
        ratio = ratio.loc[(ratio['ratio'] >= 0) & (ratio['editor_ratio'] >= 0)]
        basic.log('%s ratios' % len(ratio))
        ratio = ratio.rename(columns = {'page_id.1':'page_id'})
        merged = ratio.merge(n1,left_index=True,right_index=True,how='outer',suffixes=['_0','_1']).dropna()
        print(merged)
        #ratio = self.append_article_to_talk(ratio,write=False)
        print(len(ratio))
        print(len(merged))
        #print(merged.columns.values)
        result_path = '%s/merged_edit_ratios.csv' % (self.db_path)
        merged = merged.rename(columns = {'title_1':'title',
                                          'lang_1':'lang'})
        columns = ['page_id_1','title','len_1','no_revert_len_1','num_editors_1','td_1','tds_1','lang','page_id_0','len_0','no_revert_len_0','num_editors_0','td_0','tds_0','ratio','editor_ratio']
        merged.to_csv(result_path,na_rep='NaN',columns=columns,encoding='utf-8')
        #print(ratio)
        return merged
        
    def user_rev_size(self):
        f_in_name = '%s/combined_raw_edits.csv' % self.db_path
        f_in = pd.read_csv(f_in_name)
        columns = ['user_id','user_text']
        results = {}
        for n in self.namespace:
            for r in self.revert:
                h = '%s_%s' % (n,r)
                columns.append(h)
                if n == 'at':
                    result[self.lang][n][r][s] = df[r].sum()
                else:
                    result[self.lang][n][r][s] = df.loc[(df['namespace'] == self.namespace.index(n)),r].sum()
        nr = f_in.loc[(f_in['revert'] == False)]
        df = f_in[['page_id','title','namespace']].drop_duplicates(subset='page_id').set_index('page_id',drop=False)
        s = f_in['page_id'].value_counts().to_frame('len')
        nrs = nr['page_id'].value_counts().to_frame('no_revert_len')
        result = df.join(s).join(nrs)
        result_path = '%s/edit_counts.csv' % (self.db_path)
        result.to_csv(result_path,na_rep='NaN',columns=columns,encoding='utf-8')
        if v:
            print(result)
        return result

    def document_robustness_checks(self,f_in):
        basic.log('running document tests')
        df = pd.read_csv(f_in,escapechar='\\')
        assert len(df) == self.edit_count
        basic.log('passed edit count test: iteration count and document line count match')
        assert len(df['page_id'].unique()) == self.page_count
        basic.log('passed page count test: iteration count and unique page_id match')
        assert len(df.loc[df['namespace'] == 0]['title'].unique()) == len(df.loc[df['namespace'] == 0]['page_id'].unique())
        assert len(df.loc[df['namespace'] == 1]['title'].unique()) == len(df.loc[df['namespace'] == 1]['page_id'].unique())
        basic.log('passed title uniqueness test: equal number of unique titles and page_ids')
        assert len(df.loc[(df['namespace'] >= 0) & (df['namespace'] <= 1)]) == len(df)
        basic.log('passed namespace test: namespaces equal 0 or 1')


def job_script(args):
    f = open(args.job_script,'w')
    script_dir = os.path.abspath(__file__)
    langs = [name for name in os.listdir(config.ROOT_PROCESSED_DIR) if (os.path.isdir(os.path.join(config.ROOT_PROCESSED_DIR,name)) and 'combined' not in name)]
    for l in langs:
        out = 'python3 %s -l %s' % (script_dir,l)
        if args.drop1:
            out = out + ' --drop1'
        if args.counts:
            out = out + ' --counts'
        if args.link:
            out = out + ' --link'
        if args.append:
            out = out + ' --append'
        out = out + '\n'
        print(out)
        f.write(out)
    
def main():
    parser = argparse.ArgumentParser(description='process wiki data')
    parser.add_argument('-l','--lang')
    parser.add_argument('-b','--base_dir')
    parser.add_argument('-j','--job_script')
    parser.add_argument('--drop1',action='store_true')
    parser.add_argument('--append',action='store_true')
    parser.add_argument('--link',action='store_true')
    parser.add_argument('--counts',action='store_true')
    parser.add_argument('--ratio',action='store_true')
    #parser.add_argument('-i','--infile')
    #parser.add_argument('-o','--outfile')
    args = parser.parse_args()
    if args.job_script:
        job_script(args)
    else:
        if args.base_dir:
            c = Page_Edit_Counter(args.lang,args.base_dir,drop1=args.drop1)
        else:
            c = Page_Edit_Counter(args.lang,drop1=args.drop1)
        df = None
        if args.counts:
            df = c.rev_size()
        if args.link:
            df = c.link_documents(df)
        if args.ratio:
            df = c.edit_ratios(df)
        elif args.append:
            df = c.append_article_to_talk(df)
                
if __name__ == "__main__":
    main()
