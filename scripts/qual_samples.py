from hpc_analyzer import Analyzer
import os
import pandas as pd

def get_files(base_dir,target_name,debug=False):
    files = []
    for root, directories, filenames in os.walk(base_dir):
        for filename in filenames:
            if target_name in filename and 'simple' not in root and 'combine' not in root:
                files.append(os.path.join(root,filename))
    if debug:
        print(files)
        return None
    else:
        return files

def get_quantiles(lang,quantiles):
    ns = ['t']
    r = ['len']
    drop1 = True
    a = Analyzer(lang,ns,r,drop1)
    q = a.edit_quantiles(write=False)[ns[0]][r[0]]['quantiles']
    result = q.loc[quantiles]
    return result

def get_pages(quantile,edit_counts,n):
    sample = edit_counts.loc[(edit_counts['namespace'] == 1) & (edit_counts['len'] > quantile)].sample(n=n)
    print(sample)
    return sample

def read_edit_counts(path):
    edit_counts = pd.read_csv(path)
    edit_counts.len = edit_counts.len.astype(float)
    print(edit_counts)
    return edit_counts
    
def main():
    files = get_files('../db/','edit_counts',True)
    #get_quantiles('simple',[.5,.9,.99])
    for f in files:
        d = read_edit_counts(f)
        get_pages(100,d,2)
    
if __name__ == "__main__":
    main()
