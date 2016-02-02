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
    parser.add_argument('base_dir')
    parser.add_argument('f_out')
    parser.add_argument('files',nargs='+')
    args = parser.parse_args()

    c = Combine_Dumps(args.files,args.f_out,args.base_dir)
    c.combine()

if __name__ == "__main__":
    main()
    

