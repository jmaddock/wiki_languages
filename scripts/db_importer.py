from mw import xml_dump,Timestamp
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
                basic.log('finished import')
                return True
            
            for page in self.dh.dump:
                if page.namespace == 1 or page.namespace == 0:
                    page_list.append(self.create_document(page).copy())
                    self.count += 1
                if len(page_list) >= 1000:
                    self.c.insert(page_list)
                    if v:
                        basic.log('inserted %s documents' % self.count)
                    page_list = []
            self.c.insert(page_list)
            page_list=[]

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
        for i,p in enumerate(talk_pages):
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
            if i % 1000 == 0 and i != 0:
                basic.log('processed %s documents' % i)

def main():
    langs = ['fr',]
    for lang in langs:
        dbi = Db_Importer(lang)
        dbi.insert_from_dump(v=True)
        dbi.link_documents()
                
if __name__ == "__main__":
    main()
