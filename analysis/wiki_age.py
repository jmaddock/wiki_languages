import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
import datetime
import utils
import argparse
import config

def get_num_editors(infile_name):
    # read raw edit csv
    df = utils.read_wiki_edits_file(infile_name)
    # get language from file path
    lang = infile.replace(config.ROOT_PROCESSED_DIR,'').replace(config.COMBINED_RAW_EDITS,'').replace('/','')
    # get the total number of editors
    num_editors_no_IP = len(edit_df.loc[edit_df['user_id'] != 'None']['user_id'].unique())
    num_editors_include_anon = len(edit_df.loc['user_text'].unique())
    result = pd.DataFrame([{'lang':lang,
                            'num_editors_no_anon':num_editors_no_IP,
                            'num_editors_include_anon':num_editors_include_anon}])
    return result

def get_lang_dirs_from_config():
    # get list of languages from directory structure
    lang_list = [name for name in os.listdir(config.ROOT_PROCESSED_DIR) if (os.path.isdir(os.path.join(config.ROOT_PROCESSED_DIR,name)) and 'combined' not in name)]
    # get full file path
    file_list = [os.path.join(config.ROOT_PROCESSED_DIR,lang,config.COMBINED_RAW_EDITS) for lang in lang_list]
    return file_list

def main():
    parser = argparse.ArgumentParser(description='process wiki data')
    parser.add_argument('-o','--outfile',
                        help='file path for sorted csv output')
    args = parser.parse_args()
    
    df = pd.DataFrame()
    # get list of file paths
    infile_list = get_lang_dirs_from_config()
    for infile in infile_list:
        utils.log('processing {0}'.format(infile))
        # get the datetime of the last edit
        df = df.append(get_num_editors(infile))
    print(df)
    df.to_csv(args.outfile,index=False)

if __name__ == "__main__":
    main()
