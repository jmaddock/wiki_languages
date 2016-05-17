import config
import os
import pandas as pd

fname = os.path.join(config.ROOT_PROCESSED_DIR,'en',config.EDIT_COUNTS)
df = pd.read_csv(fname,na_values={'title':''},keep_default_na=False,dtype={'title': object})
df = df.set_index('page_id',drop=False)
df = df.set_value(2474652, 'title', 'Sam Vincent (disambiguation)')
df = df.set_value(1877838, 'title', 'Bugatti Chiron (disambiguation)')
df = df.drop([1826283,27902244])
df.to_csv(fname,na_rep='NaN',encoding='utf-8')
