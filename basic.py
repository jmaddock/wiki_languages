from mw import xml_dump
from collections import Counter
import codecs
import re

p_user = re.compile('\[\[User:([^\s\|]+)\|')
p_topic = re.compile('== .*? ==')

def make_user_count(text,page):
    user_count = Counter()
    user_list = [x for x in re.findall(p_user,text)]
    '''if 'Cromwellt|Cromwellt]]' in user_list:
        print(page.title)
        print(text)'''
    user_count.update(user_list)
    return user_count

def split_topics(text):
    topic_list = re.split(p_topic,text)
    for x in topic_list:
        print(x)
    return topic_list

def write_results(f_name):
    f_path = os.path.join(os.path.dirname(__file__),os.pardir,'results/') + f_name
    f = open(f_path, 'w')
    return f

def open_dump(wiki_name):
    f_in = r'/Users/klogg/research_data/wiki_dumps/dumps.wikimedia.org/%s/latest/%s-latest-pages-meta-current.xml' % wiki_name
    print(f_in)
    dump = xml_dump.Iterator.from_file(codecs.open(f_in,'r','utf-8'))
    return dump

dump = open_dump('simplewiki')
count = 0
user_count = Counter()
for page in dump:
    if page.namespace==1:
        count += 1
        #print(page.namespace,page.title)
        for text in page:
            #user_count = user_count + make_user_count(text.text,page)
            split_topics(text.text)
            print('---')
    if count > 0:
        break
#print(len(user_count))
#for user in user_count.most_common():
#    print(user)

print('done')
