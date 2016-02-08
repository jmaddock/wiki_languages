def single_import_from_dump(self,f_in,f_out,v=False):
        inserted_count = 0
        self.dh = single_dump_handler.Single_Dump_Handler(self.wiki_name,f_in)
        db_file = open(f_in,'w')
        db_file.write('"page_id","namespace","title","user_text","user_id","revert","ts"\n')
        self.dh.process_dump()
        for i,page in enumerate(self.dh.dump):
            if page.namespace == 1 or page.namespace == 0:
                self.create_csv_document(page,db_file)
                inserted_count += 1
                if v and inserted_count % 1000 == 0 and inserted_count != 0:
                    basic.log('inserted (insert) %s documents' % inserted_count)
            if i % 1000 == 0 and i != 0 and v:
                basic.log('processed (insert) %s documents' % i)
        basic.write_log('inserted %s documents' % inserted_count)

            
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
