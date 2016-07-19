## rename this file 'config.py'

import os

ROOT_RAW_XML_DIR = '/Users/klogg/research_data/wiki_language_data/dumps/'
ROOT_PROCESSED_DIR = '/Users/klogg/research_data/wiki_language_data/processed/'
SCRIPT_DIR = os.path.abspath(__file__)

## File Names
RAW_EDITS_BASE = 'raw_edits_'
COMBINED_RAW_EDITS = 'combined_raw_edits.csv'
EDIT_COUNTS = 'edit_counts.csv'
EDIT_COUNTS_DROP1 = 'edit_counts_drop1.csv'
LINKED_EDIT_COUNTS = 'linked_edit_counts.csv'
MERGED_EDIT_COUNTS = 'only_talk_edit_counts_edit_counts.csv'
MERGED_EDIT_RATIOS = 'full_proccessed_only_talk_edit_counts.csv'
MERGED_EDIT_RATIOS_DROP1 = 'full_proccessed_only_talk_edit_counts_drop1.csv'

COMBINED_EDIT_RATIOS = 'all_langs_full_proccessed_only_talk_edit_counts.csv'
COMBINED_EDIT_RATIOS_NO_TITLES = 'all_langs_full_proccessed_only_talk_edit_counts_no_titles.csv'

DROP1_SUFFIX = 'drop1'

## character to use instead of double quote in CSVs
QUOTE_ESCAPE_CHAR = '&quot'

## data files
BOT_LIST = '/Users/klogg/dev/wiki_languages/dump_processing_pipeline/bot_list.csv'
LANG_LIST = '/Users/klogg/dev/wiki_languages/dump_processing_pipeline/langs.csv'
