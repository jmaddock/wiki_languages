from mw import xml_dump,Timestamp
from collections import Counter
import pandas as pd
import os
import datetime
import basic
import json
import argparse

## TO START MONGOD INSTANCE ON OMNI:
## mongod --dbpath ~/jim/wiki_data/mongodb_data/ --fork --logpath ~/jim/wiki_data/mongodb_data/logs/mongodb.log

class File_Importer(object):
    def __init__(self,wiki_name,db_path=None):
        basic.log('creating importer...')
        self.wiki_name = wiki_name
        self.dh = None
        if not db_path:
            self.db_path = os.path.join(os.path.dirname(__file__),os.pardir,'db/') + wiki_name
        else:
            self.db_path = db_path
        self.create_db_dir()

    def create_db_dir(self):
        if not os.path.exists(self.db_path):
            print('creating dir: %s' % self.db_path)
            os.makedirs(self.db_path)

    def import_from_dump(self,v=False):
        inserted_count = 0
        self.dh = basic.Dump_Handler(self.wiki_name,history=True)
        db_file_name = '%s/raw_edits.csv' % self.db_path
        db_file = open(db_file_name,'w')
        db_file.write('"page_id","namespace","title","user_text","user_id","revert","ts"\n')
        while True:
            try:
                self.dh.next_dump()
            except OSError:
                basic.log('file does not exist (import finished?), exiting importer...')
                basic.write_log('file does not exist (import finished?), exiting importer...')
                return True
            
            for i,page in enumerate(self.dh.dump):
                if page.namespace == 1 or page.namespace == 0:
                    self.create_csv_document(page,db_file)
                    inserted_count += 1
                    if v and inserted_count % 1000 == 0 and inserted_count != 0:
                        basic.log('inserted (insert) %s documents' % inserted_count)
                if i % 1000 == 0 and i != 0 and v:
                    basic.log('processed (insert) %s documents' % i)
            basic.write_log('inserted %s documents' % inserted_count)

    def import_csv_from_file(self):
        self.db = pd.read_csv(self.db_file_name)

    def create_json_document(self,page):
        rt = basic.Revert_Tracker()
        d = {}
        d['page_id'] = page.id
        d['namespace'] = page.namespace
        d['title'] = page.title.split(':')[-1]
        d['linked_pages'] = []
        d['rev'] = []
        for rev in page:
            r = {}
            r['user_text'] = rev.contributor.user_text
            r['user_id'] = rev.contributor.id
            r['revert'] = rt.is_revert(rev.sha1)
            r['ts'] = str(datetime.datetime.fromtimestamp(rev.timestamp))
            d['rev'].append(r)
        return d

    def create_csv_document(self,page,db_file):
        rt = basic.Revert_Tracker()
        d = {}
        d['page_id'] = page.id
        d['namespace'] = page.namespace
        d['title'] = page.title.split(':')[-1].replace('"','')
        for rev in page:
            r = {}
            if rev.contributor.user_text:
                r['user_text'] = rev.contributor.user_text.replace('"','')
            else:
                r['user_text'] = rev.contributor.user_text
            r['user_id'] = rev.contributor.id
            r['revert'] = rt.is_revert(rev.sha1)
            r['ts'] = str(datetime.datetime.fromtimestamp(rev.timestamp))
            result = '%s,%s,"%s","%s",%s,"%s","%s"\n' % (d['page_id'],
                                                         d['namespace'],
                                                         d['title'],
                                                         r['user_text'],
                                                         r['user_id'],
                                                         r['revert'],
                                                         r['ts'])
            db_file.write(result)

    def link_documents(self,v=False):
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
        result_path = '%s/linked_edit_counts.csv' % (self.db_path)
        columns = ['title','namespace','len','no_revert_len','linked_id']
        result.to_csv(result_path,na_rep='NONE',columns=columns,encoding='utf-8')
        return result
        #print(result.loc[(result['title'] == 'April')])
        #print(result.loc[result['page_id.1'] == 1])
        
        
    def add_rev_size(self,v=False):
        f_in_name = '%s/raw_edits.csv' % self.db_path
        f_in = pd.read_csv(f_in_name)
        count = 0
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
        print(result.loc[(result['title'] == 'April') & (result['namespace'] == 0)])
        if v:
            print(result)
        return result

    def test(self):
        print(f_in.loc[(f_fin['title'] == 'April')])
        
'''
class User_Db_Importer(Db_Importer):
    def __init__(self,wiki_name):
        Db_Importer.__init__(self,wiki_name)
        self.udb = self.client['user_edits']
        self.uc = self.udb[self.wiki_name]

    def make_user_documents(self,v=False):
        pages = self.c.find({'rev_len.no_revert':{'$gt':1}}).limit(10)
        count = 0
        for p in pages:
            print(p)
            user_list = set()
            for r in p['rev']:
                if count == 1:
                    self.udb.c.create_index('user_id')
                    if v:
                        basic.log('creating user_id index...')
                if r['user_id'] not in user_list:
                    user = self.uc.find_one({'user_id':r['user_id']})
                    if not user:
                        new_user = self.create_user(r)
                        self.uc.insert(new_user)
                new_rev = self.add_rev(p,r)
                self.uc.update({'user_id':r['user_id']},
                               {'$addToSet':{'edits':new_rev}})
                user_list.add(r['user_id'])
                count += 1
                if v and count % 1000 == 0 and count != 0:
                    basic.log('processed %s revisions' % count)
        
    def create_user(self,r):
        user = {
            'user_id':r['user_id'],
            'user_text':r['user_text'],
            'edits':[]
        }
        return user

    def add_rev(self,p,r):
        edit = {
            'ts':r['ts'],
            'revert':r['revert'],
            'page_id':p['page_id'],
            '_id':p['_id'],
            'namespace':p['namespace']
        }
        if len(p['linked_pages']) > 0:
            edit['linked_page_id'] = p['linked_pages'][0]['page_id']
        return edit

    def create_edit_counts(self):
        pages = self.uc.find().limit(2)
        for p in pages:
            c = Counter()
            unique_a = set()
            unique_t = set()
            for e in p['edits']:
                print(e['namespace'])
                c.update([str(e['namespace'])])
                print(c)
                if e['namespace'] == 0:
                    unique_a.add(e['page_id'])
                else:
                    unique_t.add(e['page_id'])
            result = {
                'talk_edits':c['1'],
                'article_edits':c['0'],
                'talk_edited':len(unique_t),
                'articles_edited':len(unique_a)
            }
            print(result)
            self.uc.update({'_id':p['_id']},
                           {'$set':result})
'''
            
def main():
    parser = argparse.ArgumentParser(description='process wiki data')
    parser.add_argument('lang')
    args = parser.parse_args()

    fi = File_Importer(args.lang)
    fi.import_from_dump()
    #fi.import_csv_from_file()
    #fi.test()
    fi.add_rev_size()
    fi.link_documents()
    #dbi.link_documents()
    #dbi.add_rev_size(v=True)
    #udi = User_Db_Importer(lang)
    #udi.make_user_documents(v=True)
    #udi.create_edit_counts()
                
if __name__ == "__main__":
    main()
