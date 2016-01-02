from mw import xml_dump,Timestamp
import datetime
import basic
import pymongo

## TO START MONGOD INSTANCE ON OMNI:
## mongod --dbpath ~/jim/wiki_data/mongodb_data/ --fork --logpath ~/jim/wiki_data/mongodb_data/logs/mongodb.log

class Db_Importer(object):
    def __init__(self,wiki_name):
        basic.log('creating importer...')
        self.dh = basic.Dump_Handler(wiki_name,history=True)
        basic.log('opened %s dump' % wiki_name)
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
        while True:
            try:
                self.dh.next_dump()
                for page in self.dh.dump:
                    if page.namespace == 1 or page.namespace == 0:
                        page_list.append(self.create_document(page))
                        self.count += 1
                    if len(page_list) >= 1000:
                        self.c.insert(page_list)
                        if v:
                            basic.log('inserted %s documents' % self.count)
                        page_list = []
                self.c.insert(page_list)
            except OSError:
                basic.log('finished import')
                return True

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
        talk_pages = self.c.find()
        for i,p in enumerate(talk_pages):
            linked = self.c.find_one({'title':p.title,
                                      'namespace':{'$ne':p.namespace}})
            if linked:
                l = {}
                l['id'] = linked['id']
                l['namespace'] = linked['namespace']
                l['page_id'] = linked['page_id']
                self.c.update({'id':p['id']},
                              {'$push':{'linked_pages':l}})
            if i % 1000 == 0:
                basic.log('processed %s documents' % i)

def main():
    dbi = Db_Importer('sv')
    dbi.insert_from_dump(v=True)
    dbi.link_documents()
                
if __name__ == "__main__":
    main()
