from combine_processed_dumps import Combine_Dumps
import os
import argparse

class Combine_Stats(Combine_Dumps):
    def __init__(self,base_dir,f_out):        
        Combine_Dumps.__init__(self,None,f_out,base_dir,True)

    def get_files(self,base_dir):
        files = [f for f in os.listdir(base_dir)]
        return files

## Use either --lang or --base_dir flag
## TODO: create auto file name parser
def main():
    parser = argparse.ArgumentParser(description='process wiki data')
    parser.add_argument('-b','--base_dir')
    parser.add_argument('-o','--outfile')
    args = parser.parse_args()
    
    c = Combine_Stats(args.base_dir,args.outfile)
    c.combine()

if __name__ == "__main__":
    main()
    
