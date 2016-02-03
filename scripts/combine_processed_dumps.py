import os
import argparse
import pandas as pd

class Combine_Dumps(object):
    def __init__(self,files,f_out,base_dir):
        self.base_dir = base_dir
        self.files = files
        self.f_out = f_out

    def combine(self):
        for i,f in enumerate(self.files):
            f_in = self.base_dir + f
            if i == 0:
                result = pd.read_csv(f_in)
            else:
                result = result.append(pd.read_csv(f_in))
        result.to_csv(self.base_dir+self.f_out)

def main():
    parser = argparse.ArgumentParser(description='process wiki data')
    parser.add_argument('-b','--base_dir')
    parser.add_argument('-l','--lang')
    parser.add_argument('-o','--outfile',nargs=1)
    parser.add_argument('-f','--files',nargs='+')
    args = parser.parse_args()
    if args.base_dir:
        base_dir = args.base_dir
    elif args.lang:
        base_dir = os.path.join(os.path.dirname(__file__),os.pardir,'db/%s/' % (args.lang))
    c = Combine_Dumps(args.files,args.outfile,args.base_dir)
    c.combine()

if __name__ == "__main__":
    main()
    

