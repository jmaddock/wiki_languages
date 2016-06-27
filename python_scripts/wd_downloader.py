import urllib
import os
import argparse
import utils
import config
import pandas as pd

class WD_Downloader(object):

    def __init__(self,target_dir,langs):
        self.target_dir = target_dir
        if langs[0][-4:] == '.csv':
            self.langs = self.parse_langs(langs)
        else:
            self.langs = langs

    def make_dirs(self):
        for lang in self.langs:
            new_dir = r'%s/%s' % (self.target_dir,lang)
            if not os.path.exists(new_dir):
                utils.log('creating dir: %s' % new_dir)
                os.makedirs(new_dir)

    def download(self,file_type,date):
        wd = urllib.URLopener()
        for lang in self.langs:
            utils.log('downloading %s' % lang)
            counter = 0
            exit_loop = False
            while not exit_loop:
                if counter == 0:
                    base_url = r'https://dumps.wikimedia.org/%swiki/%s/%swiki-%s-pages-meta-%s.xml.7z' % (lang,date,lang,date,file_type)
                    target_file = r'%s/%s/%swiki-%s-pages-meta-%s%s.xml.7z' % (self.target_dir,lang,date,lang,date,file_type,(counter+1))
                else:
                    if self.lang == 'en':
                        base_url = r'https://dumps.wikimedia.org/%swiki/%s/%swiki-%s-pages-meta-%s%s.xml*.7z' % (lang,date,lang,date,file_type,counter)
                        target_file = r'%s/%s/%swiki-%s-pages-meta-%s%s.xml*.7z' % (self.target_dir,lang,date,lang,date,file_type,counter)
                    else:
                        base_url = r'https://dumps.wikimedia.org/%swiki/%s/%swiki-%s-pages-meta-%s%s.xml.7z' % (lang,date,lang,date,file_type,counter)
                        target_file = r'%s/%s/%swiki-%s-pages-meta-%s%s.xml.7z' % (self.target_dir,lang,date,lang,date,file_type,counter)
                utils.log('url: %s' % base_url)
                utils.log('target file: %s' % target_file)
                try:
                    wd.retrieve(base_url,target_file)

                except IOError:
                    if counter > 0:
                        exit_loop = True
                counter += 1

    # TODO: parse langs from top langs csv
    def parse_langs(self,lang_file):
        lang_list = pd.read_csv(lang_file,index_col=False)
        return list(lang_list['lang'].values

def main():
    parser = argparse.ArgumentParser(description='download wiki data')
    #parser.add_argument('-b','--target_dir')
    parser.add_argument('-d','--date')
    parser.add_argument('-l','--langs',nargs='+')
    parser.add_argument('--history',action='store_true')
    parser.add_argument('--current',action='store_true')
    args = parser.parse_args()
    d = WD_Downloader(config.ROOT_RAW_XML_DIR,args.langs)
    d.make_dirs()
    if args.date:
        date = str(args.date)
    else:
        date = 'latest'
    if args.history:
        d.download('history',date)
    if args.current:
        d.download('current',date)

if __name__ == "__main__":
    main()
