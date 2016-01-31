from mw import xml_dump,Timestamp
from collections import Counter
import datetime
import basic
import pymongo

## TO START MONGOD INSTANCE ON OMNI:
## mongod --dbpath ~/jim/wiki_data/mongodb_data/ --fork --logpath ~/jim/wiki_data/mongodb_data/logs/mongodb.log

class Db_Importer(object):
    def __init__(self,wiki_name):
        basic.log('creating importer...')
        self.wiki_name = wiki_name
        self.dh = None
        self.client = pymongo.MongoClient()
        self.db = self.client['edit_history']
        self.c = self.db[wiki_name]
        try:
            self.count = self.c.find().sort('id',-1).limit(1).next()['id'] + 1
        except StopIteration:
            self.count = 0
        basic.log('found %s documents in %s collection' % (self.count,wiki_name))

    def insert_from_dump(self,v=False):
        page_list = []
        self.dh = basic.Dump_Handler(self.wiki_name,history=True)
        while True:
            try:
                self.dh.next_dump()
            except OSError:
                basic.log('file does not exist (import finished?), exiting importer...')
                basic.write_log('file does not exist (import finished?), exiting importer...')
                return True
            
            for i,page in enumerate(self.dh.dump):
                if page.namespace == 1 or page.namespace == 0:
                    page_list.append(self.create_document(page).copy())
                    self.count += 1
                if len(page_list) >= 1000:
                    self.c.insert(page_list)
                    if v:
                        basic.log('inserted %s documents' % self.count)
                    page_list = []
                if i % 1000 == 0 and i != 0 and v:
                    basic.log('processed (insert) %s documents' % i)
            self.c.insert(page_list)
            page_list=[]
            basic.write_log('inserted %s documents' % self.count)

    def create_document(self,page):
        rt = basic.Revert_Tracker()
        d = {}
        d['id'] = self.count
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
            r['ts'] = datetime.datetime.fromtimestamp(rev.timestamp)
            d['rev'].append(r)
        return d

    def link_documents(self,v=False):
        basic.log('creating indexes...')
        self.c.create_index([('title',pymongo.ASCENDING),
                             ('namespace',pymongo.ASCENDING)])
        #self.c.create_index('id')
        talk_pages = self.c.find({'linked_pages':{'$size':0},
                                  'namespace':1})
        i = 0
        j = 0
        for p in talk_pages:
            linked = self.c.find_one({'title':p['title'],
                                      'namespace':0})
            if linked:
                l1 = {}
                l1['id'] = linked['id']
                l1['namespace'] = linked['namespace']
                l1['page_id'] = linked['page_id']
                self.c.update({'_id':p['_id']},
                              {'$addToSet':{'linked_pages':l1}})
                l2 = {}
                l2['id'] = p['id']
                l2['namespace'] = p['namespace']
                l2['page_id'] = p['page_id']
                self.c.update({'_id':linked['_id']},
                              {'$addToSet':{'linked_pages':l2}})
                j += 1
            i += 1
            if i % 1000 == 0 and i != 0:
                basic.log('processed (link) %s documents' % i)
        basic.write_log('processed (link) %s documents' % i)
        basic.write_log('linked %s documents' % j)

    def add_rev_size(self,v=False):
        pages = self.c.find()
        count = 0
        for p in pages:
            revert = len(p['rev'])
            no_revert = 0
            for rev in p['rev']:
                if not rev['revert']:
                    no_revert += 1
            size = {'revert':revert,
                    'no_revert':no_revert}
            self.c.update({'_id':p['_id']},
                          {'$set':{'rev_len':size}})
            count += 1
            if count % 1000 == 0 and count != 0 and v:
                basic.log('updated (rev len) %s documents' % count)
        if v:
            basic.log('updated (rev len) %s documents' % count)

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
            
            
def main():
    langs = ['simple']
    for lang in langs:
        #dbi = Db_Importer(lang)
        #dbi.insert_from_dump(v=True)
        #dbi.link_documents()
        #dbi.add_rev_size(v=True)
        udi = User_Db_Importer(lang)
        udi.make_user_documents(v=True)
        udi.create_edit_counts()
                
if __name__ == "__main__":
    main()
