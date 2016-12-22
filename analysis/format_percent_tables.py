import pandas as pd
import argparse

def format_percent_table(infile):
    df = pd.read_table(infile,header=1,index_col=0,na_values='.')
    df = df.reset_index(level=0)
    df = df.rename(columns={'index':'vars'})
    df = df.loc[(df['vars'].str.len() == 2)]
    df = df[['vars','%']]
    df = df.sort_values(by='%',ascending=False)
    df = df.rename(columns={'vars':'Language','%':'Percent Change'})
    return df

def main():
    parser = argparse.ArgumentParser(description='process wiki data')
    parser.add_argument('-i','--infile',
                        required=True,
                        help='path to a coefficient .tsv generated by STATA listcoef')
    parser.add_argument('-o','--outfile',
                        help='path to .pdf of the graph')
    args = parser.parse_args()
    df = format_percent_table(args.infile)
    if args.outfile:
        df.to_csv(args.outfile,encoding='utf-8',index=False)
    else:
        print(df)
    
if __name__ == "__main__":
    main()

