import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
import datetime
import utils
import argparse
import config
import numpy as np

## --ratio and --merge FLAGS MUST BE USED WITH LOADED .CSV FILE

## TO START MONGOD INSTANCE ON OMNI:
## mongod --dbpath ~/jim/wiki_data/mongodb_data/ --fork --logpath ~/jim/wiki_data/mongodb_data/logs/mongodb.log

SCRIPT_DIR = os.path.abspath(__file__)

class Page_Edit_Counter(object):
    def __init__(self,wiki_name,duration_bin,drop1=False,no_bots=False,date_threshold=None,relative_date_threshold=None):
        utils.log('creating importer...')
        self.wiki_name = wiki_name
        self.drop1 = drop1
        self.no_bots = no_bots
        if date_threshold:
            self.date_threshold = datetime.datetime.strptime(date_threshold,'%Y-%m-%d %H:%M:%S')
        else:
            self.date_threshold = None
        self.duration_bin = duration_bin
        self.relative_date_threshold = relative_date_threshold

    def load_raw_edit_file(self,f_in_name):
        utils.log('loading data from file %s' % f_in_name)
        # if processing EN Wikipedia, automatically load w/ iterator to avoid memory errors
        if self.wiki_name == 'en':
            tp = pd.read_csv(f_in_name,
                             na_values={'title':'','user_text':''},
                             keep_default_na=False,
                             dtype={'title': object,'author': object},
                             iterator=True,
                             chunksize=1000)
            df = pd.concat(tp, ignore_index=True)
        # if not EN Wikipedia, try loading w/out iterator, but fall back to iterator
        else:
            try:
                df = pd.read_csv(f_in_name,
                                 na_values={'title':'','user_text':''},
                                 keep_default_na=False,
                                 dtype={'title': object,'user_text':object})
            except MemoryError:
                utils.log('file too large, importing with iterator...')
                tp = pd.read_csv(f_in_name,
                                 na_values={'title':''},
                                 keep_default_na=False,
                                 dtype={'title': object},
                                 iterator=True,
                                 chunksize=1000)
                df = pd.concat(tp, ignore_index=True)
        return df

    def link_documents(self,f_in,v=False):
        utils.log('linking %s' % self.wiki_name)
        f_in.title = f_in.title.astype(str)
        n0 = f_in.loc[(f_in['namespace'] == 0)].set_index('title',drop=False)
        n1 = f_in.loc[(f_in['namespace'] == 1)].set_index('title',drop=False)
        l0 = f_in.loc[(f_in['namespace'] == 0)].set_index('title',drop=False)[['page_id']]
        l1 = f_in.loc[(f_in['namespace'] == 1)].set_index('title',drop=False)[['page_id']]
        l0.rename(columns = {'page_id':'linked_id'}, inplace = True)
        l1.rename(columns = {'page_id':'linked_id'}, inplace = True)
        result0 = n0.join(l1).set_index('page_id',drop=False)
        result1 = n1.join(l0).set_index('page_id',drop=False)
        result = result0.append(result1)
        if v:
            print(result)
        result['page_id'] = result.index
        columns = ['page_id','title','namespace','len','no_revert_len','num_editors','td','tds','lang','linked_id']
        result = self.drop_dups(result)
        result = result[columns]
        return result

    ## drop of df duplicates based on page_id field
    ## log number of duplicates dropped
    def drop_dups(self,df):
        num_dups = len(df.set_index('page_id',drop=False).index.get_duplicates())
        percent = num_dups/len(df)
        utils.log('dropped %s (%.2f%%) duplicates' % (num_dups,percent))
        return df.drop_duplicates(subset='page_id',keep=False)

    def _drop1_editors(self,df):
        df = df.loc[(df['num_editors'] > 1)]
        assert len(df.loc[(df['num_editors'] < 2)]) == 0
        utils.log('passed drop1 editor test: all pages have more than 1 editor')
        return df

    def _drop1_edits(self,df):
        df = df.loc[(df['len'] > 1)]
        assert len(df.loc[(df['len'] < 2)]) == 0
        utils.log('passed drop1 edit test: all pages have more than 1 edit')
        return df

    def _drop1(self,df):
        df = self._drop1_edits(df)
        df = self._drop1_editors(df)
        return df

    def flag_bots(self,df):
        bot_list = pd.read_csv(config.BOT_LIST,dtype={'bot_name':object},na_values={'title':''},keep_default_na=False,)
        df['is_bot'] = df['user_text'].isin(bot_list['bot_name'])
        return df

    def remove_bots(self,df):
        num_bots = len(df.loc[df['is_bot'] == True])
        percent = num_bots/len(df)
        utils.log('dropped {0} ({1:.2f}%) bot edits'.format(num_bots,percent))
        df = df.loc[df['is_bot'] == False]
        return df

    def threshold_by_date(self,df):
        # convert ts column to datetime
        df['ts'] = pd.to_datetime(df['ts'],format="%Y-%m-%d %H:%M:%S")
        # get all edits earlier than date_threshold
        df = df.loc[df['ts'] <= self.date_threshold]
        return df

    def threshold_by_relative_date(self, df):
        # get all edits earlier than date_threshold
        df = df.loc[df['ts'] <= self.relative_date_threshold]
        return df

    ## remove all rows containing an archive to get only active page ids
    ## if the page only contains archive, get a random archived ID
    def process_archive_names(self,df):
        # get all non-archived ids
        result = df.loc[df['archive'] == 'None']
        # get an archived id for each archive that doesn't have an un-archived page
        only_archive = self.get_archives_without_unarchived(df)
        print(len(only_archive))
        # concat the 2 dfs
        result = pd.concat([result,only_archive])
        return result

    def get_archives_without_unarchived(self,df):
        # get all unarchived talk page titles
        unarchived_talk_page_titles = df.loc[(df['namespace'] == 1) & (df['archive'] == 'None')]['title']
        # get all pages with titles not in unarchived_talk_page_titles
        archived_talk_pages_without_non_archives = df.loc[(~df['title'].isin(unarchived_talk_page_titles)) & (df['namespace'] == 1)]
        # remove duplicated titles (multiple archives)
        archived_talk_pages_without_non_archives = archived_talk_pages_without_non_archives.drop_duplicates('title')
        # get the number of pages
        return archived_talk_pages_without_non_archives

    ## reduce edit csv to page level csv counting edits
    ## INCLUDES TIMEDELTA AND NUM_EDITORS
    def rev_size(self,df,relative_date_threshold=None):
        # check if bot file exists and if not raise OSError
        if self.no_bots:
            if not os.path.isfile(config.BOT_LIST):
                utils.log('missing bot list file!')
                raise OSError
        utils.log('creating %s edit counts' % self.wiki_name)
        # remove all edits made by registered bots
        if self.no_bots:
            utils.log('dropping bot edits')
            df = self.flag_bots(df)
            df = self.remove_bots(df)
        if self.date_threshold:
            utils.log('applying date threshold')
            df = self.threshold_by_date(df)
        if relative_date_threshold:
            df = self.threshold_by_relative_date(df)
        # create a dataframe w/out reverted edits
        no_revert_count_df = df.loc[(df['revert'] == False)]
        # create a "result" dataframe with only non-archived pages
        result = self.process_archive_names(df)
        # drop all duplicate IDs
        result = result[['page_id','title','namespace']].drop_duplicates(subset='page_id')
        # group df by namespace and title
        # count occurrences of title
        # convert to dataframe
        # move namespace and title from index to column
        count_df = df.groupby(['namespace','title']).size().to_frame('len').reset_index()
        no_revert_count_df = no_revert_count_df.groupby(['namespace','title']).size().to_frame('no_revert_len').reset_index()
        # merge the aggregated df (including reverts) with the result df by title and namespace
        result = result.merge(count_df,on=['title','namespace'])
        # merge the aggregated df (not including reverts) with the result df by title and namespace
        # use outer join to account for articles that are all reverts
        # fill n/a values with 0
        result = result.merge(no_revert_count_df,on=['title','namespace'],how='outer')
        # fill n/a values with 0
        result[['no_revert_len']] = result[['no_revert_len']].fillna(value=0,axis=1)
        # calculate the age of a given page
        age = self.page_age(df)
        # calculate the number of editors that have contributed to a give page
        editors = self.num_editors(df)
        # merge the editor and age columns w/ the result df
        result = result.merge(age,on='page_id').merge(editors,on='page_id')
        # add language column
        result['lang'] = self.wiki_name
        # drop pages w/ single editors and single edits
        if self.drop1:
            result = self._drop1(result)
        result = result[['page_id','title','namespace','len','no_revert_len','num_editors','td','tds','lang']]
        return result

    ## find the timedelta between the first and the last edit to a page (not between edits)
    ## return reduced dataframe
    def page_age(self,df):
        utils.log('getting %s time deltas' % self.wiki_name)
        df['ts'] = pd.to_datetime(df['ts'],format="%Y-%m-%d %H:%M:%S")
        tdf = df.set_index('ts',drop=False).sort_index(ascending=True)
        first = tdf[['page_id','namespace','ts']].drop_duplicates(subset='page_id',keep='first')
        first = first.set_index('page_id',drop=False)
        last = tdf[['page_id','namespace','ts']].drop_duplicates(subset='page_id',keep='last')
        last = last.set_index('page_id',drop=False)
        result = last['ts'].subtract(first['ts'],axis='index',fill_value=-1).to_frame()
        result['tds'] = result['ts'].astype('timedelta64[s]')
        result = result.rename(columns = {'ts':'td'})
        result['page_id'] = result.index
        return result

    # get the age of each edit relative to the first edit in the dataframe
    def reletive_page_age(self,df,duration='month'):
        df['ts'] = pd.to_datetime(df['ts'], format="%Y-%m-%d %H:%M:%S")
        first_row = np.datetime64((df['ts'].min()))
        if duration == 'month':
            df['relative_age'] = df['ts'].subtract(first_row, axis='index').astype('timedelta64[M]')
        elif duration == 'year':
            df['relative_age'] = df['ts'].subtract(first_row, axis='index').astype('timedelta64[Y]')
        elif duration == 'week':
            df['relative_age'] = df['ts'].subtract(first_row, axis='index').astype('timedelta64[W]')
        else:
            df['relative_age'] = df['ts'].subtract(first_row, axis='index').astype('timedelta64[D]')
        return df

    ## get the number of unique editors who have contributed to a group of articles
    ## return reduced dataframe
    def num_editors(self,df):
        utils.log('creating %s author counts' % self.wiki_name)
        df = df.groupby('page_id').user_text.nunique().to_frame()
        df.columns = ['num_editors']
        df['page_id'] = df.index
        return df

    def edit_ratios(self,df=None,r='len'):
        utils.log('creating edit ratios %s' % self.wiki_name)
        df.page_id = df.page_id.astype(float)
        df.linked_id = df.linked_id.astype(float)
        df = df.loc[df['linked_id'].notnull()]
        df = self.drop_dups(df)
        utils.log('dropped %s duplicates' % len(df.set_index('page_id',drop=False).index.get_duplicates()))
        df = df.drop_duplicates(subset='page_id',keep=False)
        utils.log('%s' % (self.wiki_name))
        utils.log('%s pages' % len(df))
        n0 = df.loc[(df['namespace'] == 0) & (~df['linked_id'].isnull())].set_index('page_id',drop=False)
        n1 = df.loc[(df['namespace'] == 1) & (~df['linked_id'].isnull())].set_index('linked_id',drop=False)
        utils.log('%s articles' % len(n0))
        utils.log('%s talk' % len(n1))
        ratio = n0[r].divide(n1[r],axis='index',fill_value=-1).to_frame()
        ratio.columns = ['ratio']
        ratio.ratio = ratio.ratio.astype(int)
        editor_ratio = n0['num_editors'].divide(n1['num_editors'],axis='index',fill_value=-1).to_frame()
        editor_ratio.columns = ['editor_ratio']
        editor_ratio.editor_ratio = editor_ratio.editor_ratio.astype(int)
        ratio = n0.join(ratio).join(editor_ratio).set_index('page_id')
        ratio = ratio.loc[(ratio['ratio'] >= 0) & (ratio['editor_ratio'] >= 0)]
        utils.log('%s ratios' % len(ratio))
        ratio = ratio.rename(columns = {'page_id.1':'page_id'})
        merged = ratio.merge(n1,left_index=True,right_index=True,how='outer',suffixes=['_0','_1'])
        merged = merged.rename(columns = {'title_1':'title',
                                          'lang_1':'lang',
                                          'page_id':'page_id_1',
                                          'linked_id_1':'page_id_0'})
        columns = ['page_id_1','title','len_1','no_revert_len_1','num_editors_1','td_1','tds_1','lang','page_id_0','len_0','no_revert_len_0','num_editors_0','td_0','tds_0']
        merged = merged[columns]
        return merged

class Clean_En(object):
    def __init__(self,df=None):
        if isinstance(df, pd.DataFrame):
            self.df = df
        else:
            fname = os.path.join(config.ROOT_PROCESSED_DIR,'en',config.EDIT_COUNTS)
            self.df = pd.read_csv(fname,na_values={'title':''},keep_default_na=False,dtype={'title': object})
        self.dropped_articles = 0
        
    def clean(self):
        df = self.df
        df = df.set_index('page_id',drop=False)
        try:
            df = df.set_value(2474652, 'title', 'Sam Vincent (disambiguation)')
        except ValueError:
            utils.log('page id 27902244 not contained in df')
        try:
            df = df.set_value(1877838, 'title', 'Bugatti Chiron (disambiguation)')
        except ValueError:
            utils.log('page id 1877838 not contained in df')
        try:
            df = df.drop(1826283)
            self.dropped_articles += 1
        except ValueError:
            utils.log('page id 1826283 not contained in df')
        try:
            df = df.drop(27902244)
            self.dropped_articles += 1
        except ValueError:
            utils.log('page id 27902244 not contained in df')
        return df
## --ratio and --merge FLAGS MUST BE USED WITH LOADED .CSV FILE

def job_script(args):
    # create the job script file, passed in command line params with -j flag
    f = open(args.job_script,'w')
    # get a list of language dirs if lang isn't specified
    langs = [name for name in os.listdir(args.base_dir) if (os.path.isdir(os.path.join(args.base_dir,name)) and 'combined' not in name)]
    for l in langs:
        infile_path = os.path.join(args.base_dir,l,'combined_raw_edits.csv')
        out = 'python3 {0} -l {1} -i {2} -o {3}'.format(SCRIPT_DIR,
                                                        l,
                                                        infile_path,
                                                        args.outdir)
        if args.drop1:
            out = out + ' --drop1'
        if args.no_bots:
            out = out + ' --no_bots'
        if args.date_threshold:
            out = out + ' --date_threshold "{0}"'.format(args.date_threshold)
        if args.duration_bin:
            out = out + ' --duration_bin {0}'.format(args.duration_bin)
        out = out + '\n'
        print(out)
        f.write(out)

def main():
    parser = argparse.ArgumentParser(description='process wiki data')
    parser.add_argument('-l','--lang',
                        help='the two letter wiki language codes to to process')
    parser.add_argument('-i', '--infile',
                        help='path to a file of raw edits')
    parser.add_argument('-o', '--outdir',
                        help='path to the output file')
    parser.add_argument('--duration_bin',
                        choices=['day','week','month','year'],
                        help='the duration of relative age to bin by')
    parser.add_argument('--date_threshold',
                        help='only include edits before a set date.  use format Y-m-d H:M:S (e.g. 2015-10-03 07:23:40)')
    parser.add_argument('--relative_date_threshold',
                        help='number of [d,w,m,y] to include in the output after the first edit')
    parser.add_argument('--drop1',action='store_true',
                        help='drop all articles with single edits and single editors')
    parser.add_argument('--no_bots',action='store_true',
                        help='drop all bot edits from counts')
    parser.add_argument('-j', '--job_script',
                        help='generate a job script to the specified output path/file')
    parser.add_argument('-b','--base_dir',
                        help='base dir containing language directories (use for job script)')
    args = parser.parse_args()
    if args.job_script:
        job_script(args)
    else:
        c = Page_Edit_Counter(wiki_name=args.lang,
                              duration_bin=args.duration_bin,
                              drop1=args.drop1,
                              no_bots=args.no_bots,
                              date_threshold=args.date_threshold,
                              relative_date_threshold=args.relative_date_threshold)
        raw_edit_df = c.load_raw_edit_file(args.infile)
        raw_edit_df = c.reletive_page_age(df=raw_edit_df,
                                          duration=args.duration_bin)
        max_relative_age = int(raw_edit_df['relative_age'].max())
        utils.log('found {0} relative date bins'.format(max_relative_age))
        for i in range(max_relative_age):
            utils.log('creating df for relative date threshold: {0}'.format(i))
            df = c.rev_size(df=raw_edit_df,
                            relative_date_threshold=i)
            if args.lang == 'en':
                clean_en = Clean_En(df)
                df = clean_en.clean()
            df = c.link_documents(df)
            df = c.edit_ratios(df)
            outfile_name = os.path.join(args.outdir,'_{0}_{1}.csv'.format(args.lang,i))
            utils.log('writing file {0}'.format(outfile_name))
            df.to_csv(outfile_name,na_rep='NaN',encoding='utf-8',index=False)

if __name__ == "__main__":
    main()
