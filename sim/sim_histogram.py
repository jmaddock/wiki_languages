import pandas as pd
import argparse
import matplotlib.pyplot as plt
import matplotlib
from ggplot import *

matplotlib.style.use('ggplot')

def graph_histogram(simulated_infile,observed_infile,var_name):
    simulated_df = pd.read_csv(simulated_infile)
    # get summary stats of simulated distribution
    result_df = simulated_df.describe().transpose()
    # read the actual observed coefs
    observed_df = pd.read_table(observed_infile,index_col=0,header=1).transpose()
    # change the index
    result_df['vars'] = result_df.index
    observed_df['vars'] = observed_df.index
    # join the observed values with with the summary simulated values
    # use this sheet for drawing arrows
    result_df = result_df.merge(observed_df[['vars','b']],on='vars')
    # create the df for the histogram which includes both simulated and observed coef values
    histogram_df = simulated_df.append(pd.to_numeric(observed_df['b']).to_frame('b').transpose())

    # label the x axis
    xlabel = 'Simulated Coefficient Value'
    # label the y axis
    ylabel = 'Frequency'
    # get the minimum simulated coef value
    sim_min = result_df.loc[result_df['vars'] == var_name]['min'].values[0]
    # get the median simulated coef value
    sim_median = result_df.loc[result_df['vars'] == var_name]['50%'].values[0]
    # get the maximum simulated coef value
    sim_max = result_df.loc[result_df['vars'] == var_name]['max'].values[0]
    # get the actual simulated coef value
    observed_value = result_df.loc[result_df['vars'] == var_name]['b'].values[0]
    # create the histogram
    ax = histogram_df[[var_name]].plot.hist(legend=False,
                                            title=None,
                                            figsize=(7,3),
                                            bins=50)
    # set axis labels
    ax.set_ylabel(ylabel)
    ax.set_xlabel(xlabel)
    # create arrows for min, median, max, and observed coef values
    ax.annotate('A',xy=(sim_min,0),xytext=(sim_min,50),arrowprops=dict(facecolor='black', shrink=0.05))
    ax.annotate('B',xy=(sim_median,0),xytext=(sim_median,50),arrowprops=dict(facecolor='black', shrink=0.05))
    ax.annotate('C',xy=(sim_max,0),xytext=(sim_max,50),arrowprops=dict(facecolor='black', shrink=0.05))
    ax.annotate('D',xy=(observed_value,0),xytext=(observed_value,50),arrowprops=dict(facecolor='black', shrink=0.05))
    fig = ax.get_figure()
    return fig
    
def main():
    parser = argparse.ArgumentParser(description='process wiki data')
    parser.add_argument('-s','--simulated_infile',
                        required=True,
                        help='.csv summary of simulated coefficient values and observed coefficient values')
    parser.add_argument('-a','--actual_infile',
                        required=True,
                        help='.tsv of actual coef values')
    parser.add_argument('-o','--outfile',
                        help='path to .pdf of the graph')
    parser.add_argument('-v','--var_name',
                        help='name of the variable to plot')
    parser.add_argument('--show_graph',
                        action='store_true',
                        help='flag to plot graph in new window')
    args = parser.parse_args()
    fig = graph_histogram(simulated_infile=args.simulated_infile,
                          observed_infile=args.actual_infile,
                          var_name=args.var_name)
    if args.outfile:
        fig.savefig(args.outfile)
    if args.show_graph:
        plt.show()
    
if __name__ == "__main__":
    main()
