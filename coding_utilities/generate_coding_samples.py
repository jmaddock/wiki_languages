from hpc_analyzer import Analyzer
import os
import utils
import argparse
import pandas as pd

SEED = 100

def get_files(base_dir,target_name,debug=False):
    files = []
    for root, directories, filenames in os.walk(base_dir):
        for filename in filenames:
            if debug:
                if target_name == filename and 'combine' not in root:
                    files.append({'lang':root.rsplit('/',1)[1],'path':os.path.join(root,filename)})
            else:
                if target_name == filename and 'combine' not in root and 'simple' not in root:
                    files.append({'lang':root.rsplit('/',1)[1],'path':os.path.join(root,filename)})
    if debug:
        print(files)
    return files

class Qual_Sampler(object):
    def __init__(self,lang,edit_count_path,threshold_values,threshold_by,sample_by,num_pages,debug=False):
        # if debug option set verbose output
        if debug:
            self.v = True
        else:
            self.v = False
        # the language to sample from
        self.lang = lang
        # set threshold_by (edits/editors)
        self.threshold_by = threshold_by
        if 'edits' in sample_by:
            self.sample_by = 'len'
        elif 'editors' in sample_by:
            self.sample_by = 'num_editors'
        if 'quantiles' in self.threshold_by:
            # create quantile values from talk pages
            # can be changed to 'a' for articles
            self.ns = ['t']
            # drop single edits from quantile calculation
            self.drop1 = True
            # calculate the full list of percentiles for the language based off of
            # editors or edits
            self.full_quantile_list = self.find_all_quantiles()
            # calculate the absolute values of edits or editors from quantiles
            self.absolute_threshold_values = self.shorten_quantile_list(threshold_values) #TODO
        elif 'values' in self.threshold_by:
            self.absolute_threshold_values = self.threshold_values_from_absolutes(threshold_values) #TODO
        # raw csv of edit counts per page (e.g. from 'linked_edit_counts.csv)
        self.edit_counts = self.read_edit_counts(edit_count_path)
        # number of pages to sample
        self.num_pages = num_pages

    # return a formatted list of absolute values from threshold_values argument
    # list is correct format for self.get_pages
    def threshold_values_from_absolutes(self,threshold_values):
        threshold_values = pd.DataFrame({'values':threshold_values})
        threshold_values.set_index(keys='values',drop=False)
        threshold_values.index.name = 'quantiles'
        return threshold_values

    def find_all_quantiles(self):
        a = Analyzer(self.lang,self.ns,[self.sample_by],self.drop1)
        q = a.edit_quantiles(write=False)[self.ns[0]][self.sample_by]['quantiles']
        q.index = q.index.astype(float)
        q.columns = ['values']
        q.values = q.values.astype(float)
        q.index.name = 'quantiles'
        if self.v:
            pd.set_option('display.height', 150)
            print('full quantile list: {0}'.format(q))
        return q

    def read_edit_counts(self,path):
        edit_counts = pd.read_csv(path)
        edit_counts.len = edit_counts.len.astype(float)
        #if self.v:
        #    print('edit count file: {0}'.format(edit_counts))
        return edit_counts

    def shorten_quantile_list(self,threshold_values):
        result = self.full_quantile_list.iloc[[x-1 for x in threshold_values]]
        result.columns = ['values']
        result.index.name = 'quantiles'
        if self.v:
            print('shortened quantile list: {0}'.format(result))
        return result

    def quantile_labels_from_all_quantiles(self,values,v=False):
        result = self.full_quantile_list.loc[(self.full_quantile_list['values'] < int(values+1))].tail(1)
        result.columns = ['values']
        result.index.name = 'quantiles'
        if v:
            print(result)
        return result


    ## takes a dataframe of quantiles and dataframe of edit_counts
    def get_pages(self,v=False):
        utils.log('sampling from {0}'.format(self.lang))
        sample_list = pd.DataFrame()
        for i, q in self.absolute_threshold_values.iterrows():
            y = q['values']
            x = self.num_pages
            while x > 0:
                try:
                    sample = self.edit_counts.loc[(self.edit_counts['namespace'] == 1) & (self.edit_counts[self.sample_by] == y)].sample(n=x,random_state=SEED)
                    if self.threshold_by == 'quantiles':
                        quantile_label = self.quantile_labels_from_all_quantiles(values=y).index.values[0]
                    else:
                        quantile_label = y
                except ValueError:
                    sample = self.edit_counts.loc[(self.edit_counts['namespace'] == 1) & (self.edit_counts[self.sample_by] == y)]
                    if self.threshold_by == 'quantiles':
                        quantile_label = self.quantile_labels_from_all_quantiles(values=y).index.values[0]
                    else:
                        quantile_label = y
                    utils.log('not enough data in {0} {1}, len {2} for lang: {3}'.format(self.threshold_by,quantile_label,y,self.edit_counts.iloc[0]['lang']))
                sample = sample.assign(sample_from=quantile_label)
                x = x - len(sample)
                y += 1
                if v:
                    print(sample)
                sample_list = sample_list.append(sample)
                # limit number of tries to 100 percentile or 100* the given value
                if (self.threshold_by == 'quantiles' and quantile_label >= 1) or (self.threshold_by == 'values' and quantile_lable == q['values']):
                    utils.log('not enough pages in {0}, aborting...'.self.lang)
                    break
        sample_list = sample_list.assign(url = lambda x : (r'https://'+x.lang+r'.wikipedia.org/wiki/'+x.title))
        if v:
            print(sample_list)
        return sample_list

    def write_to_csv(self,df,output_dir):
        meta_columns = ['page_id','lang','len','title','url','sample_from']
        coding_columns = ['templating','index_box','heading','posts','unique_authors','depth','archive','exchanges']
        columns = meta_columns + coding_columns
        #utils.create_dir(coding)
        df.to_csv(os.path.join(output_dir,'{0}_codes.csv'.format(self.lang)),encoding='utf-8',columns=columns)
    
def main():
    parser = argparse.ArgumentParser(description='process wiki dumps')
    parser.add_argument('-f','--file_name',
                        help='base file name for generating quantiles',
                        default='linked_edit_counts.csv')
    parser.add_argument('-i','--input_dir',
                        required=True,
                        help='base directory that contains all language subdirectories')
    parser.add_argument('-o','--output_dir',
                        required=True,
                        help='output directory for coding sheets')
    parser.add_argument('-n','--num_pages',
                        type=int,
                        help='number of pages to sample from each threshold point')
    parser.add_argument('-v','--threshold_values',
                        nargs='+',
                        required=True,
                        type=int,
                        help='a list of threshold points (quantiles or absolute values)')
    parser.add_argument('-t','--threshold_by',
                        required=True,
                        choices=['quantiles','values'],
                        help='threshold by quantiles or values')
    parser.add_argument('-s','--sample_by',
                        required=True,
                        choices=['edits','editors'],
                        help='sample by edits or editors')
    parser.add_argument('-d','--debug',
                        action='store_true',
                        help='include simple_english wiki for debuging purposes, set verbose to true')
    args = parser.parse_args()
    files = get_files(args.input_dir,args.file_name,args.debug)#,v=True)
    for f in files:
        qs = Qual_Sampler(f['lang'],
                          f['path'],
                          args.threshold_values,
                          args.threshold_by,
                          args.sample_by,
                          args.num_pages,
                          args.debug)
        p = qs.get_pages()
        qs.write_to_csv(p,args.output_dir)
    
if __name__ == "__main__":
    main()
