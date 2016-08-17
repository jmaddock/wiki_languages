import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from mw import xml_dump
import pandas as pd
import datetime
import utils
import subprocess
import codecs
import argparse
import config
import translations

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
        utils.log('opening file: %s' % self.f_in)
        self.dump = xml_dump.Iterator.from_file(codecs.open(self.uncompressed,'r','utf-8'))
        return self.dump

    def decompress(self):
        utils.log('decompressing file: %s' % self.f_in)
        subprocess.call(['7z','x',self.f_in,'-o' + self.base_dir])

    def remove_dump(self):
        self.dump = None
        utils.log('removing file: %s' % self.uncompressed)
        subprocess.call(['rm',self.uncompressed])

    def process_dump(self):
        if os.path.exists(self.uncompressed):
            self.remove_dump()
        self.decompress()
        self.open_dump()

class CSV_Creator(object):
    def __init__(self,lang):
        utils.log('creating importer...')
        self.lang = lang
        self.dh = None
        self.edit_count = 0
        self.page_count = 0
        self.db_path = config.ROOT_PROCESSED_DIR

    def create_db_dir(self):
        utils.log(self.db_path+self.lang)
        if not os.path.exists(self.db_path+self.lang):
            utils.log('creating dir: %s' % self.db_path+self.lang)
            os.makedirs(self.db_path+self.lang)

    def single_import_from_dump(self,f_in=None,f_out=None,n=None,v=False,debug=False):
        self.dh = Single_Dump_Handler(f_in)
        db_file = open(f_out,'w')
        db_file.write('"page_id","namespace","title","archive","user_text","user_id","revert","ts"\n')
        if debug is not None:
            self.dh.open_dump()
        else:
            self.dh.process_dump()
            
        for page in self.dh.dump:
            if page.namespace == 1 or page.namespace == 0:
                self.create_csv_document(page,db_file)
            if debug and debug > 0 and debug == self.page_count:
                break
        utils.log('processed %s pages and %s edits' % (self.page_count,self.edit_count))
        if not debug:
            self.dh.remove_dump()

    def create_csv_document(self,page,db_file):
        # create a list to track already used edit hashes
        rt = Revert_Tracker()
        # results dictionary for each page
        d = {}
        # get the page id
        d['page_id'] = page.id
        # get the namespace (0 or 1)
        d['namespace'] = page.namespace
        # if the page is a talk page, strip "Talk:" from the title
        if page.namespace == 1:
            stripped_title = page.title.split(':',1)[-1]
        else:
            stripped_title = page.title
        # replace quote chars with an escape character, remove trailing spaces, and convert to lowercase
        stripped_title = stripped_title.replace('"',config.QUOTE_ESCAPE_CHAR).strip()#.lower()
        # remove trailing "/archive" from the title
        # get the archive number or title (if any)
        if len(stripped_title.split('/{0}'.format(translations.translations['archive'][self.lang]))) > 1 and page.namespace == 1:
            d['title'] = stripped_title.split('/{0}'.format(translations.translations['archive'][self.lang]))[0]
            d['archive'] = stripped_title.split('/{0}'.format(translations.translations['archive'][self.lang]))[1].strip()
            # if no text follows "/archive" in the title, add a 0
            if len(d['archive']) < 1:
                d['archive'] = 0
        else:
            d['archive'] = None
            d['title'] = stripped_title
        self.page_count += 1
        for rev in page:
            r = {}
            # replace quote chars in user text
            if rev.contributor.user_text:
                r['user_text'] = rev.contributor.user_text.replace('"','')
            else:
                r['user_text'] = rev.contributor.user_text
            # get the user id
            r['user_id'] = rev.contributor.id
            # determine if the revert hash has been used previously, set revert to True or False
            r['revert'] = rt.is_revert(rev.sha1)
            # get the datetime of the edit
            r['ts'] = str(datetime.datetime.fromtimestamp(rev.timestamp))
            result = '%s,%s,"%s","%s","%s",%s,"%s","%s"\n' % (d['page_id'],
                                                              d['namespace'],
                                                              d['title'],
                                                              d['archive'],
                                                              r['user_text'],
                                                              r['user_id'],
                                                              r['revert'],
                                                              r['ts'])
            self.edit_count += 1
            db_file.write(result)

    def document_robustness_checks(self,f_in):
        utils.log('running document tests')
        df = pd.read_csv(f_in,na_values={'title':''},keep_default_na=False)
        assert len(df) == self.edit_count
        utils.log('passed edit count test: iteration count and document line count match')
        assert len(df['page_id'].unique()) == self.page_count
        utils.log('passed page count test: iteration count and unique page_id match')
        #print(len(df.loc[df['namespace'] == 0]['title'].unique()),len(df.loc[df['namespace'] == 0]['page_id'].unique()))
        #titles = df.loc[df['namespace'] == 0].drop_duplicates('title')
        #ids = df.loc[df['namespace'] == 0].drop_duplicates('page_id')
        #print((ids.loc[~ids['page_id'].isin(titles['page_id'])]))
        #print(df.loc[df['page_id'] == 41])
        #print(df.loc[df['title'].str.contains('American')])
        assert len(df.loc[df['namespace'] == 0]['title'].unique()) == len(df.loc[df['namespace'] == 0]['page_id'].unique())
        assert len(df.loc[(df['namespace'] == 1) & (df['archive'] == 'None')]['title'].unique()) == len(df.loc[(df['namespace'] == 1) & (df['archive'] == 'None')]['page_id'].unique())
        utils.log('passed title uniqueness test: equal number of unique titles and page_ids')
        assert len(df.loc[(df['namespace'] >= 0) & (df['namespace'] <= 1)]) == len(df)
        utils.log('passed namespace test: namespaces equal 0 or 1')

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
        utils.log(file_list)
        for i,f in enumerate(file_list):
            outfile_name = '{0}{1}.csv'.format(config.RAW_EDITS_BASE,i+1)
            outfile_path = os.path.join(config.ROOT_PROCESSED_DIR,l,outfile_name)
            out = 'python3 {0} -l {1} -i {2} -o {3}\n'.format(SCRIPT_DIR,l,f,outfile_path)
            job_script.write(out)

def main():
    parser = argparse.ArgumentParser(description='process wiki dumps')
    parser.add_argument('-l','--lang',
                        nargs='*',
                        help='the language of the xml dump')
    parser.add_argument('-i','--infile',
                        help='the file path of a wikipedia xml dump to process')
    parser.add_argument('-o','--outfile',
                        help='a file path for an output .csv of edits')
    parser.add_argument('-j','--job_script',
                        help='the path to output a job script file for HYAK batch processing')
    parser.add_argument('-d','--debug',
                        type=int,
                        help='the number of iterations through the file xml dump to process, enter 0 to process the entire dump but do not decompress file')
    args = parser.parse_args()
    if args.job_script:
        job_script(args.job_script,args.lang)
    else:
        c = CSV_Creator(args.lang[0])
        infile = args.infile
        outfile = args.outfile
        c.single_import_from_dump(infile,outfile,debug=args.debug)
        c.document_robustness_checks(outfile)
    
if __name__ == "__main__":
    main()
