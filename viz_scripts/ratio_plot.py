import pandas as pd
import argparse
import matplotlib.pyplot as plt
import matplotlib
from ggplot import *

matplotlib.style.use('ggplot')

## Graph the factor or percent change specified by each coeficient
## INFILE: path to a coefficient .tsv generated by STATA listcoef
## OUTFILE: path to .pdf of the graph
## COEF_TYPE: percent or factor
## GROUP BY: a .csv file with 2 columns, lang and order by
def graph_ratios(infile):
    df = pd.read_csv(infile)
    df = df.reset_index(level=0)
    df = df.sort_values(by='median')
    df = df.rename(columns={'index':'vars'})
    y_column = 'median'
    title = None
    ylabel = 'Median Article/Talk Ratio'
    df = df.loc[df['lang'] != 'all']
    ax = df.plot(x='lang',
                 y=y_column,
                 kind='bar',
                 legend=False,
                 title=None,
                 figsize=(7,3))
    ax.set_ylabel(ylabel)
    ax.set_xlabel("Language")
    fig = ax.get_figure()
    return fig
    
def main():
    parser = argparse.ArgumentParser(description='process wiki data')
    parser.add_argument('-i','--infile',
                        required=True,
                        help='path to a coefficient .tsv generated by STATA listcoef')
    parser.add_argument('-o','--outfile',
                        help='path to .pdf of the graph')
    parser.add_argument('--show_graph',
                        action='store_true',
                        help='flag to plot graph in new window')
    args = parser.parse_args()
    fig = graph_ratios(args.infile)
    if args.outfile:
        fig.savefig(args.outfile)
    if args.show_graph:
        plt.show()
        
    
if __name__ == "__main__":
    main()
