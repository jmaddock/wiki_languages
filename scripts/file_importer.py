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
    def __init__(self,wiki_name,db_path=None,namespace=['a','t','at'],revert=['len','no_revert_len']):
        basic.log('creating importer...')
        self.namespace = namespace
        self.revert = revert
        self.wiki_name = wiki_name
        if not db_path:
            self.db_path = os.path.join(os.path.dirname(__file__),os.pardir,'db/') + wiki_name
        else:
            self.db_path = db_path
        self.create_db_dir()

    def create_db_dir(self):
        if not os.path.exists(self.db_path):
            print('creating dir: %s' % self.db_path)
            os.makedirs(self.db_path)

    def link_documents(self,v=False):
        basic.log('linking %s' % self.wiki_name)
        f_in_name = '%s/edit_counts.csv' % self.db_path
        f_in = pd.read_csv(f_in_name)
        n0 = f_in.loc[(f_in['namespace'] == 0)].set_index('title',drop=False)
        n1 = f_in.loc[(f_in['namespace'] == 1)].set_index('title',drop=False)
        l0 = f_in.loc[(f_in['namespace'] == 0)].set_index('title',drop=False)[['page_id']]
        l1 = f_in.loc[(f_in['namespace'] == 1)].set_index('title',drop=False)[['page_id']]
        l0.rename(columns = {'page_id':'linked_id'}, inplace = True)
        l1.rename(columns = {'page_id':'linked_id'}, inplace = True)
        result0 = n0.join(l1).set_index('page_id')
        result1 = n1.join(l0).set_index('page_id')
        result = result0.append(result1)
        if v:
            print(result)
        result['page_id'] = result.index
        result_path = '%s/linked_edit_counts.csv' % (self.db_path)
        columns = ['title','namespace','len','no_revert_len','linked_id']
        result.to_csv(result_path,na_rep='NONE',columns=columns,encoding='utf-8')
        #d=result.duplicated('page_id',keep=False).to_frame()
        #print(result.loc[d[0] == True])
        return result
        #print(result.loc[(result['title'] == 'April')])
        #print(result.loc[result['page_id.1'] == 1])        
        
    def rev_size(self,v=False):
        basic.log('creating %s edit counts' % self.wiki_name)
        f_in_name = '%s/combined_raw_edits.csv' % self.db_path
        f_in = pd.read_csv(f_in_name)
        nr = f_in.loc[(f_in['revert'] == False)]
        df = f_in[['page_id','title','namespace']].drop_duplicates(subset='page_id').set_index('page_id',drop=False)
        s = f_in['page_id'].value_counts().to_frame('len')
        #print(s.iloc[0])
        nrs = nr['page_id'].value_counts().to_frame('no_revert_len')
        #print(nrs)
        result = df.join(s).join(nrs)
        result_path = '%s/edit_counts.csv' % (self.db_path)
        columns = ['title','namespace','len','no_revert_len']
        result.to_csv(result_path,na_rep='NONE',columns=columns,encoding='utf-8')
        if v:
            print(result)
        return result

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
        out = 'python3 %s -l %s\n' % (script_dir,l)
        print(out)
        f.write(out)
    
def main():
    parser = argparse.ArgumentParser(description='process wiki data')
    parser.add_argument('-l','--lang')
    parser.add_argument('-b','--base_dir')
    parser.add_argument('-j','--job_script')
    #parser.add_argument('-i','--infile')
    #parser.add_argument('-o','--outfile')
    args = parser.parse_args()
    if args.job_script:
        job_script(args)
    else:
        if args.base_dir:
            c = Page_Edit_Counter(args.lang,args.base_dir)
        else:
            c = Page_Edit_Counter(args.lang)
        c.rev_size()
        c.link_documents()
                
if __name__ == "__main__":
    main()
