import plotly as py
import pandas as pd
import argparse

def graph_coefs(infile,outfile,percent,factor):

    df = pd.read_table(infile,header=1,index_col=0,na_values='.')
    if not percent:
        df = df.transpose()
    df = df.reset_index(level=0)
    df = df.rename(columns={'index':'vars'})
    if percent:
        y_column = '%'
    elif factor:
        y_column = 'e^b'
    df = df.loc[df['vars'].str.len() == 2]
    df = df.sort_values(y_column)
    x = df['vars']
    y = df[y_column]
        
    fig = {
        'data': [
            {
                'x':x,
                'y':y,
                'mode': 'markers',
                'marker':{'size':'10'}
            },
        ],
        'layout': {
            'title':'test title',
            'xaxis': {'title':'languages'},
            'yaxis': {'title':'percent difference compared to English','zeroline':'True','zerolinewidth':'1',},
        },
        
    }

    # IPython notebook
    # py.iplot(fig, filename='pandas/multiple-scatter')
    py.offline.plot(fig, filename=outfile)


def main():
    parser = argparse.ArgumentParser(description='process wiki data')
    parser.add_argument('-i','--infile')
    parser.add_argument('-o','--outfile')
    parser.add_argument('--percent',action='store_true')
    parser.add_argument('--factor',action='store_true')
    args = parser.parse_args()
    graph_coefs(args.infile,args.outfile,args.percent,args.factor)
    
if __name__ == "__main__":
    main()
