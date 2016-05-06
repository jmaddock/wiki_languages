from hpc_analyzer import Analyzer
import os
import basic
import pandas as pd

ROOT_DIR = '../db/'
EDIT_COUNT_FILE_NAME = 'edit_counts.csv'
OUTPUT_DIR = '../coding/'

def get_files(base_dir,target_name,v=False):
    files = []
    for root, directories, filenames in os.walk(base_dir):
        for filename in filenames:
            if target_name == filename and 'simple' not in root and 'combine' not in root:
                files.append({'lang':root[-2:],'path':os.path.join(root,filename)})
    if v:
        print(files)
    return files

class Qual_Sampler(object):
    def __init__(self,lang,edit_count_path):
        self.lang = lang
        self.ns = ['t']
        self.r = ['len']
        self.drop1 = True
        self.edit_counts = self.read_edit_counts(edit_count_path)
        self.quantiles = self.find_all_quantiles()

    def find_all_quantiles(self,v=False):
        a = Analyzer(self.lang,self.ns,self.r,self.drop1)
        q = a.edit_quantiles(write=False)[self.ns[0]][self.r[0]]['quantiles']
        q.index = q.index.astype(float)
        q.columns = ['values']
        q.values = q.values.astype(float)
        q.index.name = 'quantiles'
        if v:
            pd.set_option('display.height', 150)
            print(q)
        return q

    def read_edit_counts(self,path,v=False):
        edit_counts = pd.read_csv(path)
        edit_counts.len = edit_counts.len.astype(float)
        if v:
            print(edit_counts)
        return edit_counts

    def get_quantiles(self,quantiles=None,values=None,v=False):
        if quantiles:
            result = self.quantiles.iloc[[x-1 for x in quantiles]]
        elif values:
            result = self.quantiles.loc[(self.quantiles['values'] < int(values+1))].tail(1)
        else:
            return None
        result.columns = ['values']
        result.index.name = 'quantiles'
        if v:
            print(result)
        return result

    ## takes a dataframe of quantiles and dataframe of edit_counts
    def get_pages(self,quantiles,n,v=False):
        sample_list = pd.DataFrame()
        for i, q in quantiles.iterrows():
            y = q['values']
            x = n
            while x > 0:
                try:
                    sample = self.edit_counts.loc[(self.edit_counts['namespace'] == 1) & (self.edit_counts['len'] == y)].sample(n=x)
                    quantile_label = self.get_quantiles(values=y).index.values[0]
                except ValueError:
                    sample = self.edit_counts.loc[(self.edit_counts['namespace'] == 1) & (self.edit_counts['len'] == y)]
                    quantile_label = self.get_quantiles(values=y).index.values[0]
                    basic.log('not enough data in quantile %s, len %s for lang: %s' % (quantile_label,y,self.edit_counts.iloc[0]['lang']))
                sample = sample.assign(quantile=quantile_label)
                x = x - len(sample)
                y += 1
                if v:
                    print(sample)
                sample_list = sample_list.append(sample)
                if quantile_label >= 1:
                    break
        sample_list = sample_list.assign(url = lambda x : (r'https://'+x.lang+r'.wikipedia.org/wiki/'+x.title))
        if v:
            print(sample_list)
        return sample_list

    def write_to_csv(self,df):
        columns = ['page_id','lang','len','quantile','title','url','templating','index_box','heading','posts','unique_authors','depth','archive','exchanges']
        basic.create_dir(coding)
        df.to_csv('%s/%s_codes.csv' % (OUTPUT_DIR,self.lang),encoding='utf-8',columns=columns)
    
def main():
    files = get_files('../db/','edit_counts.csv')#,v=True)
    for f in files:
        qs = Qual_Sampler(f['lang'],f['path'])
        quantiles = qs.get_quantiles(quantiles=[50,90])
        p = qs.get_pages(quantiles,100)
        qs.write_to_csv(p)
    
if __name__ == "__main__":
    main()
