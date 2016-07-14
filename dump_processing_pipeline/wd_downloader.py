import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from urllib import request
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
        for lang in self.langs:
            utils.log('downloading %s' % lang)
            counter = 0
            exit_loop = False
            if file_type == 'current':
                file_extension = 'bz2'
            else:
                file_extension = '7z'
            while not exit_loop:
                if counter == 0:
                    base_url = r'https://dumps.wikimedia.org/%swiki/%s/%swiki-%s-pages-meta-%s.xml.%s' % (lang,date,lang,date,file_type,file_extension)
                    target_file = r'%s/%s/%swiki-%s-pages-meta-%s%s.xml.%s' % (self.target_dir,lang,lang,date,file_type,(counter+1),file_extension)
                else:
                    if lang == 'en':
                        base_url = r'https://dumps.wikimedia.org/%swiki/%s/%swiki-%s-pages-meta-%s%s.xml*.%s' % (lang,date,lang,date,file_type,counter,file_extension)
                        target_file = r'%s/%s/%swiki-%s-pages-meta-%s%s.xml*.%s' % (self.target_dir,lang,date,lang,date,file_type,counter,file_extension)
                    else:
                        base_url = r'https://dumps.wikimedia.org/%swiki/%s/%swiki-%s-pages-meta-%s%s.xml.%s' % (lang,date,lang,date,file_type,counter,file_extension)
                        target_file = r'%s/%s/%swiki-%s-pages-meta-%s%s.xml.%s' % (self.target_dir,lang,lang,date,file_type,counter,file_extension)
                utils.log('url: %s' % base_url)
                utils.log('target file: %s' % target_file)
                try:
                    request.urlretrieve(base_url,target_file)

                except IOError:
                    if counter > 0:
                        exit_loop = True
                counter += 1

    # TODO: parse langs from top langs csv
    def parse_langs(self,lang_file):
        lang_list = pd.read_csv(lang_file,index_col=False)
        return list(lang_list['lang'].values)

def main():
    parser = argparse.ArgumentParser(description='download wiki data')
    parser.add_argument('-o','--target_dir',
                        help='output base directory (not including language, these are auto generated)')
    parser.add_argument('-d','--date',
                        default='latest',
                        type=str,
                        help='a date string of the format "yyyymmdd" or "latest" for most recent')
    parser.add_argument('-l','--langs',
                        nargs='+',
                        help='a list of two letter wiki language codes to download')
    parser.add_argument('--history',
                        action='store_true',
                        help='download the complete revision for each history')
    parser.add_argument('--current',
                        action='store_true',
                        help='only download the current revision of each page')
    args = parser.parse_args()
    if args.target_dir:
        base_dir = args.target_dir
    else:
        base_dir = config.ROOT_RAW_XML_DIR
    d = WD_Downloader(base_dir,args.langs)
    d.make_dirs()
    if args.history:
        d.download('history',args.date)
    if args.current:
        d.download('current',args.date)

if __name__ == "__main__":
    main()
