import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
import datetime
import utils
import argparse
import config

def get_last_date(infile_name):
    df = utils.read_wiki_edits_file(infile_name)
    last_date = df.ix[df['tds'].idxmax()]
    print(last_date)
    return last_date

def get_lang_dirs_from_config():
    lang_list = [name for name in os.listdir(config.ROOT_PROCESSED_DIR) if (os.path.isdir(os.path.join(config.ROOT_PROCESSED_DIR,name)) and 'combined' not in name)]
    file_list = [os.path.join(config.ROOT_PROCESSED_DIR,lang,config.EDIT_COUNTS) for lang in lang_list]
    return file_list

def main():
    parser = argparse.ArgumentParser(description='process wiki data')
    parser.add_argument('-o','--outfile',
                        help='file path for sorted csv output')
    args = parser.parse_args()
    
    df = pd.DataFrame()
    infile_list = get_lang_dirs_from_config()
    for infile in infile_list:
        utils.log('processing {0}'.format(infile))
        df = df.append(get_last_date(infile))
    df.sort_values(by='tds',ascending=True)
    print(df)
    df.to_csv(args.outfile,index=False)

if __name__ == "__main__":
    main()
