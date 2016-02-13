from mw import xml_dump
import datetime
import basic
import subprocess
import codecs
import os
import argparse

class Revert_Tracker(object):
    def __init__(self):
        self.hashes = []

    def is_revert(self,new_hash):
        if new_hash in self.hashes:
            return True
        else:
            self.hashes.append(new_hash)
            return False

class Single_Dump_Handler(object):
    def __init__(self,wiki_name,f_in):
        self.wiki_name = wiki_name
        self.f_in = f_in
        self.uncompressed = f_in.rsplit('.7z', 1)[0]
        self.dump = None
        self.base_dir = f_in.rsplit('/', 1)[0]

    def open_dump(self):
        basic.log('opening file: %s' % self.f_in)
        basic.write_log('opening file: %s' % self.f_in)
        self.dump = xml_dump.Iterator.from_file(codecs.open(self.uncompressed,'r','utf-8'))
        return self.dump

    def decompress(self):
        basic.log('decompressing file: %s' % self.f_in)
        basic.write_log('decompressing file: %s' % self.f_in)
        subprocess.call(['7z','x',self.f_in,'-o' + self.base_dir])

    def remove_dump(self):
        self.dump = None
        basic.log('removing file: %s' % self.uncompressed)
        basic.write_log('removing file: %s' % self.uncompressed)
        subprocess.call(['rm',self.uncompressed])

    def process_dump(self):
        if os.path.exists(self.uncompressed):
            self.remove_dump()
        self.decompress()
        self.open_dump()

class CSV_Creator(object):
    def __init__(self,wiki_name,db_dir):
        basic.log('creating importer...')
        self.wiki_name = wiki_name
        self.dh = None
        self.edit_count = 0
        self.page_count = 0
        if db_dir:
            self.db_path = os.path.join(os.path.dirname(__file__),os.pardir,'db/')

    def create_db_dir(self):
        if not os.path.exists(self.db_path):
            print('creating dir: %s' % self.db_path)
            os.makedirs(self.db_path)

    def single_import_from_dump(self,f_in=None,f_out=None,n=None,v=False):
        self.dh = Single_Dump_Handler(self.wiki_name,f_in)
        db_file = open(f_out,'w')
        db_file.write('"page_id","namespace","title","user_text","user_id","revert","ts"\n')
        self.dh.process_dump()
        for page in self.dh.dump:
            if page.namespace == 1 or page.namespace == 0:
                self.create_csv_document(page,db_file)
                self.page_count += 1
        basic.log('processed %s pages and %s edits' % (self.page_count,self.edit_count))
        self.dh.remove_dump()

    def create_csv_document(self,page,db_file):
        rt = Revert_Tracker()
        d = {}
        d['page_id'] = page.id
        d['namespace'] = page.namespace
        #d['title'] = page.title.split(':',1)[-1].replace('"','').strip()
        d['title'] = page.title.replace('Talk:','').replace('"','').strip()
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
            self.edit_count += 1
            db_file.write(result)

def job_script(f_out):
    f = open(f_out,'w')
    script_dir = os.path.abspath(__file__)
    lang_dir = os.path.join(os.path.dirname(__file__),os.pardir,'data/')
    langs = [name for name in os.listdir(lang_dir) if os.path.isdir(lang_dir+name)]
    print(os.listdir(lang_dir))
    for l in langs:
        print(l)
        n = [x for x in os.listdir(lang_dir+l) if '.7z' in x]
        print(n)
        for i in range(1,len(n)+1):
            out = 'python3 %s/single_dump_handler.py -l %s -n %s\n' % (script_dir,l,i)
            f.write(out)
            
# use --num flag with default file structure
# only use --infile and --outfile without --num flag
def main():
    parser = argparse.ArgumentParser(description='process wiki dumps')
    parser.add_argument('-l','--lang')
    parser.add_argument('-i','--infile')
    parser.add_argument('-o','--outfile')
    parser.add_argument('-n','--num')
    parser.add_argument('-j','--job_script')
    args = parser.parse_args()
    if args.job_script:
        job_script(args.job_script)
        return None
    elif args.num and (args.infile or args.outfile):
        print('use EITHER --infile and --outfile OR --num flags')
        return None
    elif args.num:
        c = CSV_Creator(args.lang,db_dir=True)
        infile = os.path.join(os.path.dirname(__file__),os.pardir,'data/%s/%swiki-latest-pages-meta-history%s.xml.7z' % (args.lang,args.lang,args.num))
        outfile = os.path.join(os.path.dirname(__file__),os.pardir,'db/%s/raw_edits_%s.csv' % (args.lang,args.num))
        c.create_db_dir()
    else:
        c = CSV_Creator(args.lang,db_dir=False)
        infile = args.infile
        outfile = args.outfile
    c.single_import_from_dump(infile,outfile)
    
if __name__ == "__main__":
    main()
