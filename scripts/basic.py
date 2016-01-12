from mw import xml_dump,Timestamp
from collections import Counter
from difflib import Differ
from pprint import pprint
import codecs
import re
import os
import datetime
import subprocess

p_user = re.compile('\[\[User:([^\s\|]+)\|')
p_topic = re.compile('== .*? ==')
title_name = re.compile(':(.*)')

class Revert_Tracker(object):
    def __init__(self):
        self.hashes = []

    def is_revert(self,new_hash):
        if new_hash in self.hashes:
            return True
        else:
            self.hashes.append(new_hash)
            return False

class Dump_Handler(object):
    def __init__(self,wiki_name,history):
        self.base_dir = os.path.join(os.path.dirname(__file__),os.pardir,'data/') + wiki_name
        #self.base_dir = r'/Volumes/SupahFast2/jim/wiki_11_13_2015/%s' % (wiki_name)
        if history:
            self.base = '%s/%swiki-latest-pages-meta-history' % (self.base_dir,wiki_name)
            #self.base = r'/Volumes/SupahFast2/jim/wiki_11_13_2015/%s/%swiki-latest-pages-meta-history' % (wiki_name,wiki_name)
            #self.base_path = r'/Users/klogg/research_data/wiki_dumps/dumps.wikimedia.org/%s/latest/%s-latest-pages-meta-history' % (self.wiki_name,self.wiki_name)
        else:
            self.base = '%s/%swiki-latest-pages-meta-current' % (self.base_dir,wiki_name)
            #self.base = r'/Volumes/SupahFast2/jim/wiki_11_13_2015/%s/%swiki-latest-pages-meta-current' % (wiki_name,wiki_name)
            #self.base_path = r'/Users/klogg/research_data/wiki_dumps/dumps.wikimedia.org/%s/latest/%s-latest-pages-meta-current' % (self.wiki_name,self.wiki_name)
        self.base_path = None
        self.wiki_name = wiki_name
        self.history = history
        self.dump = None
        self.count = 1 #change back to 1

    def open_dump(self):
        f = r'%s.xml' % (self.base_path)
        log('opening file: %s' % f)
        write_log('opening file: %s' % f)
        self.dump = xml_dump.Iterator.from_file(codecs.open(f,'r','utf-8'))
        return self.dump

    def decompress(self):
        f = r'%s.xml.7z' % (self.base_path)
        log('decompressing file: %s' % f)
        write_log('decompressing file: %s' % f)
        subprocess.call(['7z','x',f,'-o' + self.base_dir])

    def remove_dump(self):
        self.dump = None
        f = r'%s.xml' % (self.base_path)
        log('removing file: %s' % f)
        write_log('removing file: %s' % f)
        subprocess.call(['rm',f])

    def next_dump(self):
        if self.count > 1:
            self.remove_dump()
        self.base_path = r'%s%s' % (self.base,self.count)
        if os.path.exists(r'%s.xml' % (self.base_path)):
            self.remove_dump()
        self.decompress()
        self.open_dump()
        self.count += 1
        
def log(text,log_file=None):
    print('[%s] %s' % (str(datetime.datetime.now().time())[:-7],text))
    if log_file:
        write_log(text)

def write_log(text,f_name='log.txt'):
    f_path = os.path.join(os.path.dirname(__file__),os.pardir,'logs/') + f_name
    f = open(f_path, 'a')
    out = '[%s] %s\n' % (str(datetime.datetime.now().time())[:-7],text)
    f.write(out)
    f.close()

# only takes meta-current (no revision history)
def make_user_count(page,*kwargs):
    rev = next(page._Page__revisions)
    user_count = Counter()
    user_list = [x for x in re.findall(p_user,rev.text)]
    '''if page.title=='Talk:Web browser':
        print(page.title)
        print(text)'''
    user_count.update(user_list)
    return user_count

def make_user_count_history(page,*kwargs):
    user_count = Counter()
    for rev in page:
        '''if page.title=='Talk:Web browser':
        print(page.title)
        print(text)'''
        user_count.update([rev.contributor.user_text])
    return user_count

# only takes meta-current (no revision history)
def split_topics(page,*kwargs):
    rev = page.Revision.next()
    print(rev.text)
    topic_list = re.split(p_topic,rev.text)
    '''for x in topic_list:
        print(x)'''
    return topic_list

def write_to_results(f_name):
    f_path = os.path.join(os.path.dirname(__file__),os.pardir,'results/') + f_name
    f = open(f_path, 'w')
    return f

def open_dump(wiki_name,history=False):
    if history:
        f_in = r'/Users/klogg/research_data/wiki_dumps/dumps.wikimedia.org/%s/latest/%s-latest-pages-meta-history.xml' % (wiki_name,wiki_name)
    else:
        f_in = r'/Users/klogg/research_data/wiki_dumps/dumps.wikimedia.org/%s/latest/%s-latest-pages-meta-current.xml' % (wiki_name,wiki_name)
    dump = xml_dump.Iterator.from_file(codecs.open(f_in,'r','utf-8'))
    return dump

# count function takes name and page text as params and returns iterable
#
def xcount_per_article(dump,wiki_name,count_function,f_name=None,dl=None):
    if dl:
        count = 0
    results = Counter()
    if f_name:
        f = write_to_results(('%s_%s.csv' % (f_name,wiki_name)))
        header = '"%s","%s"\n' % ('article_name','num_x')
        f.write(header)
    for page in dump:
        if page.namespace==1:
            #for rev in page:
            num_x = len(count_function(page,wiki_name)) # change for different count functions
            results.update({page.title:num_x})
            if dl:
                count += 1
                if count >= dl:
                    break
    if f_name:
        for x in results.most_common():
            out = '"%s",%i\n' % (x[0],x[1])
            f.write(out)
    return results

def align_article_talk(page,wiki_name,*kwargs):
    dump = open_dump(wiki_name,True)
    for page2 in dump:
        t1 = page.title.split(':')[-1]
        t2 = page2.title.split(':')[-1]
        if t1 == t2 and page.namespace != page2.namespace:
            return [page,page2]
    return None

def return_list(x,y):
    x.append(y)
    return x

def match_edits(talk_page,wiki_name,time_delta=None):
    pages = align_article_talk(talk_page,wiki_name)
    users = {}
    results = Counter() #update this to return whole pages
    if time_delta:
        ts_delta = datetime.timedelta(days=time_delta)
    for rev in pages[0]:
        user_text = rev.contributor.user_text
        ts = datetime.datetime.fromtimestamp(rev.timestamp)
        users[user_text] = return_list(users.get(user_text,[]),ts)
    for rev in pages[1]:
        rev_ts = datetime.datetime.fromtimestamp(rev.timestamp)
        if rev.contributor.user_text in users.keys():
            for talk_ts in users[rev.contributor.user_text]:
                if time_delta:
                    start_ts = talk_ts - ts_delta
                    end_ts = talk_ts + ts_delta
                    if rev_ts > start_ts and rev_ts < end_ts:
                        results.update([rev.contributor.user_text])
                else:
                    results.update([rev.contributor.user_text])
    return results

def main():

    wiki_name = 'simplewiki'
    dump = open_dump(wiki_name)
    hdump = open_dump(wiki_name,True)
    results = xcount_per_article(hdump,
                                 wiki_name,
                                 match_edits,
                                 f_name='article_talk_edits',
                                 dl=None)
    hdump = open_dump(wiki_name,True)
    results2 = xcount_per_article(hdump,
                                  wiki_name,
                                  make_user_count_history,
                                  f_name='number_of_users',
                                  dl=None)
    print(results.most_common())
    print(results2.most_common())

    '''count = 0
    user_count = Counter()
    d = Differ()
    for page in dump:
        if page.namespace==1:
            count += 1
            print(page.namespace,page.title)
            prev_lines = ['']
            r = Revert_Tracker()
            print(match_edits(page,wiki_name))
            for rev in page:
                print(r.is_revert(rev.sha1))
                #print(text.contributor,text.sha1,text.comment)
                lines = [x.strip() for x in re.split('\n|\.',rev.text)]
                print(rev.contributor.user_text)
                edit_date = datetime.datetime.fromtimestamp(rev.timestamp)
                print(edit_date+datetime.timedelta(days=1))
                for x in list(d.compare(prev_lines, lines)):
                    if x[:1] == '+' or x[:1] == '-':
                        print(x)
                prev_lines = lines
                #user_count = user_count + make_user_count(text.text,page)
                #split_topics(text.text)
                print('---')
        if count > 10:
            break'''
    #print(len(user_count))
    #for user in user_count.most_common():
    #    print(user)'''

    print('done')

if __name__ == "__main__":
    main()
