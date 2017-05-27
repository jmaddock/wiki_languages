
# coding: utf-8

# In[113]:

import pandas as pd
get_ipython().magic(u'matplotlib inline')

import matplotlib.pyplot as plt
from matplotlib import colors as mcolors
import numpy as np
matplotlib.style.use('ggplot')


# In[166]:

highlighted_lang_list = ['fa','pt','he']
non_highlight_color = '#777777'


# In[168]:

df = pd.read_table('/srv/wiki_language_data/stata_files/talk_article_lang_model_percents.tsv',header=1,index_col=0,na_values='.')
df = df.reset_index().rename(columns={'index':'label'})
plot_df = df.loc[df['label'].isin(highlighted_lang_list)][['label','%']]
plot_df = plot_df.append(pd.DataFrame([{'label':'en',
                                        '%':0}]))
small_plot_df = df.loc[(df['label'].str.len() == 2) & (~df['label'].isin(highlighted_lang_list))][['label','%']]


# In[169]:

plot_df


# In[170]:

small_plot_df


# In[125]:

fig = plt.figure(figsize=(7,1))
plt.plot(plot_df['%'],
         np.zeros(len(plot_df)),
         '.',
         markersize=15,
         c=('#E24A33', '#348ABD', '#988ED5', '#777777'))
plt.errorbar(x=0,
             y=0,
             xerr=[75],
             lw=1,
             ecolor='gray',
             fmt='none')
plt.tick_params(
    axis='y',          
    which='both',      
    right='off',      
    left='off',
    labelleft='off')
plt.xticks(np.arange(start=-75,stop=80,step=25))
plt.ylim(-1, 1)
plt.xlim(-85, 85)
#plt.ylabel('Language Edition')
plt.xlabel('Percent Change Compared to English')
plt.tight_layout()
fig.savefig('/home/sjm668/5-26-17_ICA_line_plot.pdf')


# In[177]:

fig = plt.figure(figsize=(7,1.5))
for i,c in zip(range(len(plot_df)),plt.rcParams['axes.prop_cycle']):
    if c['color'] == non_highlight_color:
        print(plt.rcParams['axes.prop_cycle'])
    plt.plot(plot_df.iloc[[i]]['%'],
             0,
             '.',
             markersize=25,
             c=c['color'])
plt.errorbar(x=0,
             y=0,
             xerr=[75],
             lw=1,
             ecolor='gray',
             fmt='none')
plt.tick_params(
    axis='y',          
    which='both',      
    right='off',      
    left='off',
    labelleft='off')
plt.xticks(np.arange(start=-75,stop=80,step=25))
plt.ylim(-10, 10)
plt.xlim(-85, 85)
#plt.ylabel('Language Edition')
plt.xlabel('Percent Change Compared to English')
plt.tight_layout()
fig.savefig('/home/sjm668/5-26-17_ICA_line_plot.pdf')


# In[163]:


for c in plt.rcParams['axes.prop_cycle']:
    print(c)


# In[ ]:



