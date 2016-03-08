from combine_processed_dumps import Combine_Dumps
import os
import argparse

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

## Use either --lang or --base_dir flag
## TODO: create auto file name parser
def main():
    parser = argparse.ArgumentParser(description='process wiki data')
    parser.add_argument('-b','--base_dir')
    parser.add_argument('-o','--outfile')
    parser.add_argument('--drop1',action='store_true')
    args = parser.parse_args()
    
    c = Combine_Stats(args.base_dir,args.outfile,args.drop1)
    c.combine()

if __name__ == "__main__":
    main()
    
