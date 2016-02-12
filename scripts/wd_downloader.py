import urllib
import os

class WD_Downloader(object):

    def __init__(self,target_dir,langs):
        self.target_dir = target_dir
        self.langs = langs

    def make_dirs(self):
        for lang in self.langs:
            new_dir = r'%s/%s' % (self.target_dir,lang)
            if not os.path.exists(new_dir):
                print('creating dir: %s' % new_dir)
                os.makedirs(new_dir)

    def download(self,file_type='current'):
        wd = urllib.URLopener()
        for lang in self.langs:
            print('downloading %s' % lang)
            counter = 0
            exit_loop = False
            while not exit_loop:
                if counter == 0:
                    base_url = r'https://dumps.wikimedia.org/%swiki/latest/%swiki-latest-pages-meta-%s.xml.7z' % (lang,lang,file_type)
                    target_file = r'%s/%s/%swiki-latest-pages-meta-%s%s.xml.7z' % (self.target_dir,lang,lang,file_type,(counter+1))
                else:
                    base_url = r'https://dumps.wikimedia.org/%swiki/latest/%swiki-latest-pages-meta-%s%s.xml.7z' % (lang,lang,file_type,counter)
                    target_file = r'%s/%s/%swiki-latest-pages-meta-%s%s.xml.7z' % (self.target_dir,lang,lang,file_type,counter)
                print('url: %s' % base_url)
                print('target file: %s' % target_file)
                try:
                    wd.retrieve(base_url,target_file)

                except IOError:
                    if counter > 0:
                        exit_loop = True
                counter += 1

    # TODO: parse langs from top langs csv
    def parse_langs(self,lang_file,limit):
        return None

langs = ['es','en','vi','fa','sh','he','hu','uk','ko','ca','fi','no','cs','pl',]
target_dir = os.path.join(os.path.dirname(__file__),os.pardir,'data')
#target_dir = r'/Volumes/SupahFast2/jim/wiki_11_13_2015'

d = WD_Downloader(target_dir,langs)
d.make_dirs()
d.download('history')
