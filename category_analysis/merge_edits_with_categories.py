import argparse
import pandas as pd

def combine(edits,categories):
    categories = categories.rename(columns={'id': 'page_id_0'})
    df = categories.merge(edits,on=['page_id_0','lang'],how='inner')
    return df

def limit_columns(df):
    return df[['en_category_id','en_category_title', 'page_id_0', 'lang', 'page_id_1', 'len_1', 'no_revert_len_1','num_editors_1', 'tds_1', 'len_0', 'no_revert_len_0','num_editors_0','tds_0']]

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='process wiki data')
    parser.add_argument('-e', '--edit_file',
                        required=True,
                        help='path to a file of edit counts')
    parser.add_argument('-c', '--category_file',
                        required=True,
                        help='path to a file of categories')
    parser.add_argument('o-', '--outfile',
                        required=True,
                        help='path to the output file')
    parser.add_argument('--limit_columns',
                        action='store_true',
                        help='eliminate unnecessary columns in output')

    args = parser.parse_args()
    categories = pd.read_csv(args.category_file)
    edits = pd.read_csv(args.edit_file)
    result = combine(edits=edits,categories=categories)
    if args.limit_columns:
        result = limit_columns(result)
    result.to_csv(args.outfile)