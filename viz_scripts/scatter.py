import pandas as pd
import plotly as py
import os

data_dir = '/Users/klogg/research_data/wiki_results/ratio_histogram/'
data_dir = '../results/ratio_histograms/'
files = [f for f in os.listdir(data_dir) if f[3:8] == 't_len']
#files = [f for f in os.listdir(data_dir)]
print(files)

for f in files:
    lang = f[:2]
    df = pd.read_csv(data_dir+f)
    header = df.columns.values
    x = df[header[1]].tolist()
    y = df[header[0]].tolist()

    fig = {
        'data': [
            {
                'x': x,
                'y': y,
                'name': '2007'
            },
        ],
        'layout': {
            'title':lang,
            'xaxis': {'title': 'Number of articles'},
            'yaxis': {'title': "Number of edits"}
        }
    }

    # IPython notebook
    # py.iplot(fig, filename='pandas/multiple-scatter')

    py.offline.plot(fig, filename='../viz_results/%s_t_multiple-scatter_ratio.html' % lang)
