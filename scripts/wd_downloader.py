import urllib
import os

class WD_Downloader(object):

    def __init__(self,target_dir,langs):
        self.target_dir = target_dir
        self.langs = langs

    def make_dirs(self):
        for lang in self.langs:
            new_dir = r'%s%s' % (self.target_dir,lang)
            if not os.path.exists(new_dir):
                os.makedirs(new_dir)

    def download(self,file_type='current'):
        wd = urllib.URLopener()
        for lang in self.langs:
            counter = 0
            exit_loop = False
            while not exit_loop:
                if counter == 0:
                    base_url = r'https://dumps.wikimedia.org/%swiki/latest/%swiki-latest-pages-meta-%s.xml.bz2' % (lang,lang,file_type)
                else:
                    base_url = r'https://dumps.wikimedia.org/%swiki/latest/%swiki-latest-pages-meta-%s%s.xml.bz2' % (lang,lang,file_type,counter)
                target_file = r'%s/%s/%swiki-latest-pages-meta-%s%s.xml.bz2' % (self.target_dir,lang,lang,file_type,counter)
                try:
                    wd.retrieve(base_url,target_file)
                    counter += 1
                except IOError:
                    exit_loop = True

    def parse_langs(self,lang_file,limit):
        return None

langs = ['de']
target_dir = r'/Volumes/SupahFast2/jim/test'

d = WD_Downloader(target_dir,langs)
d.make_dirs()
d.download()
