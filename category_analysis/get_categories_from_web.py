import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import utils
import pandas as pd
import numpy as np
import requests
from collections import defaultdict
import argparse

def get_en_categories():
    utils.log('getting top level english categories')
    # get first level categories from the en main page from petscan
    base_url = r'https://petscan.wmflabs.org/'
    params = {
        'language':'en',
        'project':'wikipedia',
        'depth':'0',
        'categories':'main topic classifications',
        'format':'json',
        'output_compatability':'catscan',
        'doit':'Do%20it%2',
        'ns%5B14%5D':1
    }
    r = requests.get(base_url, params=params).json()
    return r

def get_llinks(base_page_id):
    utils.log('getting language links from base page {0}'.format(base_page_id))
    # get first level categories from the en main page from petscan
    base_url = r'https://en.wikipedia.org/w/api.php'
    params = {
        'action':'query',
        'format':'json',
        'prop':'langlinks',
        'pageids':base_page_id,
        'lllimit':500,
        'formatversion':2,
        'indexpageids':None
    }

    r = requests.get(base_url, params=params).json()
    return r

def read_lang_list_file(infile):
    utils.log('reading language list from {0}'.format(infile))
    lang_list = []
    f = open(infile,'r')
    for line in f:
        lang_list.append(line.strip())
    return lang_list

def get_articles(lang,category,depth=1):
    utils.log('getting articles from {0} {1}'.format(lang,category))
    # get first level categories from the en main page from petscan
    base_url = r'https://petscan.wmflabs.org/'
    params = {
        'language':lang,
        'project':'wikipedia',
        'depth':depth,
        'categories':category,
        'format':'json',
        'output_compatability':'catscan',
        'doit':'Do%20it%2',
    }
    r = requests.get(base_url, params=params).json()
    return r

def get_article_info(lang,page_name):
    # get first level categories from the en main page from petscan
    base_url = 'https://{0}.wikipedia.org/w/api.php'.format(lang)
    params = {
        'action':'query',
        'format':'json',
        'prop':'info',
        'titles':page_name,
        'formatversion':2,
        'indexpageids':None
    }

    r = requests.get(base_url, params=params).json()
    return r

def main(lang_list=None,depth=1,min_articles=0):
    if lang_list:
        lang_list = read_lang_list_file(lang_list)
    r = get_en_categories()
    top_level_en_catagories = defaultdict(dict)
    for page in r['*'][0]['a']['*']:
        top_level_en_catagories[page['id']] = page['title']
    utils.log('found {0} top level categories'.format(len(top_level_en_catagories)))

    llink_list = defaultdict(dict)
    for category in top_level_en_catagories:
        llink_list[category]['en'] = 'Category:{0}'.format(top_level_en_catagories[category])
        r = get_llinks(category)
        for page in r['query']['pages'][0]['langlinks']:
            if (lang_list is not None and page['lang'] in lang_list) or (lang_list is None):
                llink_list[category][page['lang']] = page['title']

    result = pd.DataFrame()
    for llink in llink_list:
        for lang in llink_list[llink]:
            category_name = llink_list[llink][lang].split(':')[-1]
            category_id = get_article_info(lang, llink_list[llink][lang])['query']['pages'][0]['pageid']
            num_articles = -1
            while num_articles <= min_articles:
                utils.log('traverse depth: {0}'.format(depth))
                article_list = get_articles(lang=lang,
                                            category=category_name,
                                            depth=depth)
                num_articles = len(article_list['*'][0]['a']['*'])
                utils.log('found {0} articles'.format(num_articles))
                depth += 1
            count = 0
            for article in article_list['*'][0]['a']['*']:
                if article['namespace'] == 0:
                    result = result.append(pd.DataFrame([{
                        'id': article['id'],
                        'title': article['title'],
                        'lang': lang,
                        'category_title': llink_list[llink][lang],
                        'category_id': category_id,
                        'en_category_title': llink_list[llink]['en'],
                        'en_category_id': llink
                    }]))
                    count += 1
                if count % 1000 == 0 and count != 0:
                    utils.log('processed {0} of {1} articles'.format(count,num_articles))
            utils.log('processed {0} of {1} articles'.format(count,num_articles))
    utils.log('found {0} articles'.format(len(result)))
    return result

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='process wiki data')
    parser.add_argument('-o','--outfile',
                        help='output path')
    parser.add_argument('-l', '--lang_list',
                        help='path to a .txt file of languages to include')
    parser.add_argument('-d', '--depth',
                        default=1,
                        help='depth of the category structure to traverse')
    parser.add_argument('--min',
                        help='the minimum number of articles per category')
    args = parser.parse_args()
    result = main(lang_list=args.lang_list,
                  depth=args.depth)
    utils.log('writing file to {0}'.format(args.outfile))
    result.to_csv(args.outfile)
