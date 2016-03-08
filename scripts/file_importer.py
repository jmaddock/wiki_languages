from mw import xml_dump,Timestamp
from collections import Counter
import single_dump_handler
import pandas as pd
import os
import datetime
import basic
import json
import argparse

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

    def link_documents(self,v=False):
        basic.log('linking %s' % self.wiki_name)
        f_in_name = '%s/edit_counts.csv' % self.db_path
        f_in = pd.read_csv(f_in_name)
        if self.drop1:
            f_in = f_in.loc[(f_in['len'] > 1)]
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
        result.to_csv(result_path,na_rep='NONE',columns=columns,encoding='utf-8')
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
        f_in_name = '%s/combined_raw_edits.csv' % self.db_path
        f_in = pd.read_csv(f_in_name)
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
        result.to_csv(result_path,na_rep='NONE',columns=columns,encoding='utf-8')
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

    def num_editors(self,df):
        basic.log('creating %s author counts' % self.wiki_name)
        df = df.groupby('page_id').user_text.nunique().to_frame()
        df.columns = ['num_editors']
        df['page_id'] = df.index
        return df

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
        result.to_csv(result_path,na_rep='NONE',columns=columns,encoding='utf-8')
        if v:
            print(result)
        return result

def job_script(args):
    f = open(args.job_script,'w')
    script_dir = os.path.abspath(__file__)
    lang_dir = os.path.join(os.path.dirname(__file__),os.pardir,'db/')
    langs = [name for name in os.listdir(lang_dir) if os.path.isdir(lang_dir+name)]
    for l in langs:
        out = 'python3 %s -l %s' % (script_dir,l)
        if args.drop1:
            out = out + ' --drop1'
        out = out + '\n'
        print(out)
        f.write(out)
    
def main():
    parser = argparse.ArgumentParser(description='process wiki data')
    parser.add_argument('-l','--lang')
    parser.add_argument('-b','--base_dir')
    parser.add_argument('-j','--job_script')
    parser.add_argument('--drop1',action='store_true')
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
        c.rev_size()
        c.link_documents()
                
if __name__ == "__main__":
    main()
