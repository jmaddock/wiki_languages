from mw import xml_dump
import pandas as pd
import datetime
import basic
import subprocess
import codecs
import os
import argparse
import config

SCRIPT_DIR = os.path.abspath(__file__)

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
    def __init__(self,f_in):
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
    def __init__(self,lang):
        basic.log('creating importer...')
        self.lang = lang
        self.dh = None
        self.edit_count = 0
        self.page_count = 0
        self.db_path = config.ROOT_PROCESSED_DIR

    def create_db_dir(self):
        print(self.db_path+self.lang)
        if not os.path.exists(self.db_path+self.lang):
            print('creating dir: %s' % self.db_path+self.lang)
            os.makedirs(self.db_path+self.lang)

    def single_import_from_dump(self,f_in=None,f_out=None,n=None,v=False,debug=False):
        self.dh = Single_Dump_Handler(f_in)
        db_file = open(f_out,'w')
        db_file.write('"page_id","namespace","title","user_text","user_id","revert","ts"\n')
        if debug is not False:
            self.dh.open_dump()
        else:
            self.dh.process_dump()
        for page in self.dh.dump:
            if page.namespace == 1 or page.namespace == 0:
                self.create_csv_document(page,db_file)
                self.page_count += 1
            if debug and debug > 0 and debug == self.page_count:
                break
        basic.log('processed %s pages and %s edits' % (self.page_count,self.edit_count))
        if not debug:
            self.dh.remove_dump()

    def create_csv_document(self,page,db_file):
        rt = Revert_Tracker()
        d = {}
        d['page_id'] = page.id
        d['namespace'] = page.namespace
        if page.namespace == 1:
            d['title'] = page.title.split(':',1)[-1].replace('"','').strip()
        else:
            d['title'] = page.title.replace('"','\\"').strip()
        #d['title'] = page.title.replace('Talk:','').replace('"','').strip()
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

# IN: path and file name of job script file, optional list of languages (leave empty for all langs)
def job_script(job_script_file_name,lang_list=None):
    # create the job script file, passed in command line params with -j flag
    job_script = open(job_script_file_name,'w')
    # get a list of language dirs if lang isn't specified
    if not lang_list or len(lang_list) == 0:
        lang_list = [name for name in os.listdir(config.ROOT_RAW_XML_DIR) if os.path.isdir(os.path.join(config.ROOT_RAW_XML_DIR,name))]
    for l in lang_list:
        base_lang_dir = os.path.join(config.ROOT_RAW_XML_DIR,l)
        file_list = [os.path.join(base_lang_dir,x) for x in os.listdir(base_lang_dir) if '.7z' in x]
        print(file_list)
        for i,f in enumerate(file_list):
            out = 'python3 {0} -l {6} -i {1} -o {2}{3}/{4}{5}.csv\n'.format(SCRIPT_DIR,f,config.ROOT_PROCESSED_DIR,l,config.RAW_EDITS_BASE,i+1,l)
            job_script.write(out)

# --num flag is DEPRECIATED. use --job_script to create commands with correct --infile and --outfile flags
# use --num flag with default file structure
# only use --infile and --outfile without --num flag
def main():
    parser = argparse.ArgumentParser(description='process wiki dumps')
    parser.add_argument('-l','--lang',nargs='*')
    parser.add_argument('-i','--infile')
    parser.add_argument('-o','--outfile')
    parser.add_argument('-n','--num')
    parser.add_argument('-j','--job_script')
    parser.add_argument('-d','--debug',type=int)
    args = parser.parse_args()
    if args.job_script:
        job_script(args.job_script,args.lang)
        return None
    elif args.num and (args.infile or args.outfile):
        print('use EITHER --infile and --outfile OR --num flags')
        return None
    # DEPRECIATED
    elif args.num:
        c = CSV_Creator(args.lang)
        infile = os.path.join(os.path.dirname(__file__),os.pardir,'data/%s/%swiki-latest-pages-meta-history%s.xml.7z' % (args.lang,args.lang,args.num))
        outfile = os.path.join(os.path.dirname(__file__),os.pardir,'db/%s/raw_edits_%s.csv' % (args.lang,args.num))
        c.create_db_dir()
    else:
        c = CSV_Creator(args.lang)
        infile = args.infile
        outfile = args.outfile
    c.single_import_from_dump(infile,outfile,debug=args.debug)
    c.document_robustness_checks(outfile)
    
if __name__ == "__main__":
    main()
