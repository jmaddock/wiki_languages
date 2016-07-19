import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
import utils
import pandas as pd

fname = os.path.join(config.ROOT_PROCESSED_DIR,'en',config.EDIT_COUNTS)
df = pd.read_csv(fname,na_values={'title':''},keep_default_na=False,dtype={'title': object})
df = df.set_index('page_id',drop=False)
try:
    df = df.set_value(2474652, 'title', 'Sam Vincent (disambiguation)')
except ValueError:
    utils.log('page id 27902244 not contained in df')
try:
    df = df.set_value(1877838, 'title', 'Bugatti Chiron (disambiguation)')
except ValueError:
    utils.log('page id 1877838 not contained in df')
try:
    df = df.drop(1826283)
except ValueError:
    utils.log('page id 1826283 not contained in df')
try:
    df = df.drop(27902244)
except ValueError:
    utils.log('page id 27902244 not contained in df')
df.to_csv(fname,na_rep='NaN',encoding='utf-8')
