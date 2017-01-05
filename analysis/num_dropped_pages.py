import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
import datetime
import utils
import argparse
import config
import gc

class DroppedCounter(object):
    def __init__(self,outfile,date_threshold):
        # before initialization check for valid bot file
        self.check_bot_file()
        self.outfile = outfile
        self.infile_list = self.get_lang_dirs_from_config()
        self.dropped_count_df = None
        self.date_threshold = date_threshold

    def get_dropped_counts(self):
        # read raw edit csv
        dropped_count_df = pd.DataFrame()
        for infile in self.infile_list:
            lang = infile.replace(config.ROOT_PROCESSED_DIR,'').replace(config.COMBINED_RAW_EDITS,'').replace('/','')
            # load the raw edit file
            raw_edit_df = self.load_raw_edit_file(infile)
            # get the total number of edits
            total_edits = len(raw_edit_df)
            # mark bot edits
            raw_edit_df = self.flag_bots(raw_edit_df)
            # get number of bot edits that will be dropped
            dropped_bot_edits = self.count_dropped_bots(raw_edit_df)
            # remove bot edits
            raw_edit_df = self.remove_bots(raw_edit_df)
            # get edit/editor counts by page
            edit_count_df = self.rev_size(raw_edit_df)
            # get the total number of pages
            total_pages = len(edit_count_df)
            # count the number of editors and edits we would drop
            single_editor_edit_pages = self.drop1_count(edit_count_df)
            # actually drop those pages
            edit_count_df = self._drop1(edit_count_df)
            # link articles and talk page
            edit_count_df = self.link_documents(edit_count_df)
            # count the number of unlinked pages
            unlinked_pages = self.count_unlinked(edit_count_df)
            dropped_count_df = dropped_count_df.append(pd.DataFrame([{'total_edits':total_edits,
                                                                      'total_pages':total_pages,
                                                                      'single_editor_edit_pages':single_editor_edit_pages,
                                                                      'unlinked_pages':unlinked_pages,
                                                                      'dropped_bot_edits':dropped_bot_edits,
                                                                      'lang':lang}]))
            gc.collect()
        total = dropped_count_df.sum(numeric_only=True).to_frame().transpose()
        total['lang'] = 'total'
        dropped_count_df = dropped_count_df.append(total)
        self.dropped_count_df = dropped_count_df
        
    def check_bot_file(self):
        # check if bot file exists and if not raise OSError
        if not os.path.isfile(config.BOT_LIST):
            utils.log('missing bot list file!')
            raise OSError

    def drop_unlinked(self,df):
        return df.loc[df['linked_id'].notnull()]
        
    def count_unlinked(self,df):
        return len(df.loc[df['linked_id'].isnull()])

    def drop1_count(self,df):
        editors = len(df.loc[(df['num_editors'] <= 1)])
        df = df.loc[(df['num_editors'] > 1)]
        edits = len(df.loc[(df['len'] <= 1)])
        return edits + editors

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

    def count_dropped_bots(self,df):
        return len(df.loc[df['is_bot'] == True])

    def remove_bots(self,df):
        num_bots = len(df.loc[df['is_bot'] == True])
        percent = num_bots/len(df)
        utils.log('dropped {0} ({1:.2f}%) bot edits'.format(num_bots,percent))
        df = df.loc[df['is_bot'] == False]
        return df

    def flag_bots(self,df):
        bot_list = pd.read_csv(config.BOT_LIST,dtype={'bot_name':object},na_values={'title':''},keep_default_na=False)
        df['is_bot'] = df['user_text'].isin(bot_list['bot_name'])
        return df

    def get_lang_dirs_from_config(self):
        # get list of languages from directory structure
        lang_list = [name for name in os.listdir(config.ROOT_PROCESSED_DIR) if (os.path.isdir(os.path.join(config.ROOT_PROCESSED_DIR,name)) and 'combined' not in name)]
        # get full file path
        file_list = [os.path.join(config.ROOT_PROCESSED_DIR,lang,config.COMBINED_RAW_EDITS) for lang in lang_list]
        return file_list
    
    def load_raw_edit_file(self,infile_name):
        utils.log('loading file {0}'.format(infile_name))
        return utils.read_wiki_edits_file(infile_name)

    def link_documents(self,f_in):
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
        result['page_id'] = result.index
        return result

    def rev_size(self,df):
        utils.log('creating %s edit counts')
        if self.date_threshold:
            utils.log('applying date threshold')
            df = self.threshold_by_date(df)
        # create a dataframe w/out reverted edits
        no_revert_count_df = df.loc[(df['revert'] == False)]
        # create a "result" dataframe with only non-archived pages
        try:
            result = self.process_archive_names(df)
        except KeyError as e:
            utils.log('unable to process archives')
            result = df
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
        # drop pages w/ single editors and single edits
        return result

    def page_age(self,df):
        utils.log('getting time deltas')
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

    def process_archive_names(self,df):
        # get all non-archived ids
        result = df.loc[df['archive'] == 'None']
        # get an archived id for each archive that doesn't have an un-archived page 
        only_archive = self.get_archives_without_unarchived(df)
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

    def num_editors(self,df):
        utils.log('creating author counts')
        df = df.groupby('page_id').user_text.nunique().to_frame()
        df.columns = ['num_editors']
        df['page_id'] = df.index
        return df

    def threshold_by_date(self,df):
        # convert ts column to datetime
        df['ts'] = pd.to_datetime(df['ts'],format="%Y-%m-%d %H:%M:%S")
        # get all edits earlier than date_threshold
        df = df.loc[df['ts'] <= self.date_threshold]
        return df

    def write_results(self):
        self.dropped_count_df.to_csv(self.outfile,index=False)

def main():
    parser = argparse.ArgumentParser(description='process wiki data')
    parser.add_argument('-o','--outfile',
                        help='file path for sorted csv output')
    parser.add_argument('-d','--date_threshold',
                        help='only include edits before a set date.  use format Y-m-d H:M:S (e.g. 2015-10-03 07:23:40)')
    args = parser.parse_args()
    dropped_counter = DroppedCounter(outfile=args.outfile,
                                     date_threshold=args.date_threshold)
    dropped_counter.get_dropped_counts()
    dropped_counter.write_results()

if __name__ == "__main__":
    main()
