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
                files.append(f)
        return files
        
    def combine(self):
        for i,f in enumerate(self.files):
            f_in = self.base_dir + f
            basic.log('added %s' % f_in)
            if i == 0:
                result = pd.read_csv(f_in)
            else:
                result = result.append(pd.read_csv(f_in))
        result.to_csv(self.base_dir+self.f_out)
        basic.log('created %s' % self.f_out)

def job_script(args):
    f = open(args.job_script,'w')
    script_dir = os.path.abspath(__file__)
    lang_dir = os.path.join(os.path.dirname(__file__),os.pardir,'db/')
    langs = [name for name in os.listdir(lang_dir) if os.path.isdir(lang_dir+name)]
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
        c = Combine_Dumps(args.files,args.outfile,base_dir,args.lang)
        c.combine()

if __name__ == "__main__":
    main()
    

