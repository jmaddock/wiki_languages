# coding: utf-8

## rename this file 'config.py'

import os

### Directories ###
ROOT_RAW_XML_DIR = '/Users/klogg/dev/wiki_languages/data/'
ROOT_PROCESSED_DIR = '/Users/klogg/dev/wiki_languages/db/'


### File Names ###
# edits by page in long form (e.g. one edit per row)
RAW_EDITS_BASE = 'raw_edits_'
# each of the raw_edits_*.csv files appended to one complete file
COMBINED_RAW_EDITS = 'combined_raw_edits.csv'
# number of edits per page (e.g. one page per row)
EDIT_COUNTS = 'edit_counts.csv'
# DEPRECIATED.  CONTAINED IN "edit_counts.csv"
# edit_counts.csv with an added column linking talk page and article page id’s by title
LINKED_EDIT_COUNTS = 'linked_edit_counts.csv'
# DEPRECIATED.  CONTAINED IN "processed_edit_counts.csv"
# talk page edit counts joined to article page edit counts for modeling
MERGED_EDIT_COUNTS = 'only_talk_edit_counts.csv'
# talk page edit counts joined to article page edit counts for modeling
MERGED_EDIT_RATIOS = 'full_processed_only_talk_edit_counts.csv'

MERGED_EDIT_RATIOS_DROP1 = 'full_processed_only_talk_edit_counts_drop1.csv'

## combined file names
COMBINED_EDIT_RATIOS = 'all_langs_full_processed_only_talk_edit_counts.csv'
COMBINED_EDIT_RATIOS_NO_TITLES = 'all_langs_full_processed_only_talk_edit_counts_no_titles.csv'

QUOTE_ESCAPE_CHAR = '&quot'

## data files
BOT_LIST = 'bot_list.csv'
LANG_LIST = 'lang_list.csv'
