from hpc_analyzer import Analyzer
import os
import utils
import argparse
import pandas as pd

SEED = 100

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
                    sample = self.edit_counts.loc[(self.edit_counts['namespace'] == 1) & (self.edit_counts['len'] == y)].sample(n=x,random_state=SEED)
                    quantile_label = self.get_quantiles(values=y).index.values[0]
                except ValueError:
                    sample = self.edit_counts.loc[(self.edit_counts['namespace'] == 1) & (self.edit_counts['len'] == y)]
                    quantile_label = self.get_quantiles(values=y).index.values[0]
                    utils.log('not enough data in quantile %s, len %s for lang: %s' % (quantile_label,y,self.edit_counts.iloc[0]['lang']))
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

    def write_to_csv(self,df,output_dir):
        columns = ['page_id','lang','len','quantile','title','url','templating','index_box','heading','posts','unique_authors','depth','archive','exchanges']
        #utils.create_dir(coding)
        df.to_csv(os.path.join(output_dir,'{0}_codes.csv'.format(self.lang)),encoding='utf-8',columns=columns)
    
def main():
    parser = argparse.ArgumentParser(description='process wiki dumps')
    parser.add_argument('-f','--file_name')
    parser.add_argument('-i','--input_dir')
    parser.add_argument('-o','--output_dir')
    parser.add_argument('-n','--num_pages')
    parser.add_argument('-q','--quantile_list',nargs='*')
    args = parser.parse_args()    
    files = get_files(args.input_dir,args.file_name)#,v=True)
    for f in files:
        qs = Qual_Sampler(f['lang'],f['path'])
        if not args.quantile_list:
            quantile_list = [50,90]
        else:
            quantile_list = args.quantile_list
        quantiles = qs.get_quantiles(quantiles=quantile_list)
        p = qs.get_pages(quantiles,args.num_pages)
        qs.write_to_csv(p)
    
if __name__ == "__main__":
    main()
