import os
import argparse
import basic
import pandas as pd

class Combine_Dumps(object):
    def __init__(self,files,f_out,base_dir,lang):
        self.base_dir = base_dir
        if lang and not files:
            self.files = self.get_files(self.base_dir)
        else:
            self.files = files
        if f_out:
            self.f_out = f_out
        else:
            self.f_out = 'combined_raw_edits.csv'

    def get_files(self,db_dir):
        files = []
        for f in os.listdir(db_dir):
            if f[:10] == 'raw_edits_':
                files.append(self.base_dir+f)
        return files
        
    def combine(self):
        if self.files:
            for i,f in enumerate(self.files):
                f_in = f
                basic.log('added %s' % f_in)
                if i == 0:
                    result = pd.read_csv(f_in)
                else:
                    result = result.append(pd.read_csv(f_in))
            result.to_csv(self.f_out)
            basic.log('created %s' % self.f_out)

class Combine_Edit_Counts(Combine_Dumps):
    def __init__(self,base_dir,f_out,debug=False):
        self.f_out = f_out
        self.debug = debug
        Combine_Dumps.__init__(self,None,f_out,base_dir,True)

    def get_files(self,base_dir):
        files = []
        for root, directories, filenames in os.walk(base_dir):
            for filename in filenames:
                if 'linked_edit_counts.csv' in filename:
                    files.append(os.path.join(root,filename))
        if self.debug:
            print(files)
            return None
        else:
            return files

class Combine_Stats(Combine_Dumps):
    def __init__(self,base_dir,f_out,drop1):
        self.f_out = f_out
        self.drop1 = drop1
        Combine_Dumps.__init__(self,None,f_out,base_dir,True)

    def get_files(self,base_dir):
        print(self.f_out.split('/')[-1:])
        if self.drop1:
            files = [f for f in os.listdir(base_dir) if (f[-4:] == '.csv' and self.f_out.split('/')[-1:] not in f and 'drop1' in f)]
        else:
            files = [f for f in os.listdir(base_dir) if (f[-4:] == '.csv' and self.f_out.split('/')[-1:] not in f)]
        print(files)
        return files

def job_script(args):
    f = open(args.job_script,'w')
    script_dir = os.path.abspath(__file__)
    lang_dir = os.path.join(os.path.dirname(__file__),os.pardir,'db/')
    langs = [name for name in os.listdir(lang_dir) if (os.path.isdir(lang_dir+name) and 'combined' not in name)]
    for l in langs:
        out = 'python3 %s -l %s\n' % (script_dir,l)
        print(out)
        f.write(out)

## Use either --lang or --base_dir flag
## TODO: create auto file name parser
def main():
    parser = argparse.ArgumentParser(description='process wiki data')
    parser.add_argument('-b','--base_dir')
    parser.add_argument('-l','--lang')
    parser.add_argument('-o','--outfile')
    parser.add_argument('--dumps',action='store_true')
    parser.add_argument('--edit_counts',action='store_true')
    parser.add_argument('--stats',action='store_true')
    parser.add_argument('--drop1',action='store_true')
    parser.add_argument('--debug',action='store_true')
    parser.add_argument('-f','--files',nargs='*')
    parser.add_argument('-j','--job_script')
    args = parser.parse_args()
    if args.job_script:
        job_script(args)
    else:
        if args.base_dir:
            base_dir = args.base_dir
        elif args.lang:
            base_dir = os.path.join(os.path.dirname(__file__),os.pardir,'db/%s/' % (args.lang))

        if args.dumps:
            c = Combine_Dumps(args.files,args.outfile,base_dir,args.lang)
        elif args.edit_counts:
            c = Combine_Edit_Counts(base_dir,args.outfile,args.debug)
        elif args.stats:
            c = Combine_Stats(base_dir,args.outfile,args.drop1)
        c.combine()

if __name__ == "__main__":
    main()
    

