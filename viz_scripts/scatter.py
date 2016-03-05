import pandas as pd
import plotly as py
import os
import argparse

def scatter(data_dir,namespace=None):
    #data_dir = '/Users/klogg/research_data/wiki_results/ratio_histogram/'
    #data_dir = '../results/ratio_histograms/'
    if namespace:
        files = [f for f in os.listdir(data_dir) if f[3:8] == file_type]
    else:
        files = [f for f in os.listdir(data_dir)]
    print(files)

    for f in files:
        lang = f[:2]
        df = pd.read_csv(data_dir+f)
        header = df.columns
        print(header)
        x = df[header[0]].tolist()
        y = df[header[1]].tolist()

        fig = {
            'data': [
                {
                    'x': x,
                    'y': y,
                },
            ],
            'layout': {
                'title':lang,
                'xaxis': {'title': header[0]},
                'yaxis': {'title': header[1]}
            }
        }

        # IPython notebook
        # py.iplot(fig, filename='pandas/multiple-scatter')

        py.offline.plot(fig, filename='../viz_results/%s_multiple-scatter_ratio.html' % (lang))

def main():
    parser = argparse.ArgumentParser(description='process wiki dumps')
    parser.add_argument('-d','--data_dir')
    parser.add_argument('-n','--namespace',nargs=1)
    args = parser.parse_args()
    scatter(args.data_dir,args.namespace)

if __name__ == "__main__":
    main()

