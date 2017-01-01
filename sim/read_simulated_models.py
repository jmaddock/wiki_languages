import pandas as pd
import os
import argparse

class ModelReader(object):
    def __init__(self,indir,outfile,simulated_statistic,base_file_name,file_id_range=None):
        self.indir = indir
        self.outfile = outfile
        self.model_file_list = None
        #self.coef_labels_file = coef_labels_file
        self.combined_model_results = None
        self.simulated_statistic = simulated_statistic
        self.base_file_name = base_file_name
        self.file_id_range = file_id_range

    # create a list of file paths from indir if the file name contains base_file_name
    def get_model_file_list(self):
        if self.file_id_range:
            self.infile_list = [os.path.join(self.indir,x) for x in os.listdir(self.indir) if self.base_file_name in x and self.file_id_range[0] <= int(x.replace(self.base_file_name,'')) <= self.file_id_range[1]]
        else:
            self.infile_list = [os.path.join(self.indir,x) for x in os.listdir(self.indir) if self.base_file_name in x]
        print(self.model_file_list)

    #def get_label_mapping(self):
    #    label_df = pd.read_table(coef_lables_file,header=1,index_col=None,na_values='.')['Unnamed: 0'].values

    # read all model files
    # store simulated statistic to dataframe in combined model results
    def read_model_files(self):
        df = pd.DataFrame()
        for model_file in self.model_file_list:
            # read simulated model file
            input_df = pd.read_table(infile,header=1,index_col=None,na_values='.')
            # get the row that matches simulated statistic (probably b or s.e.)
            input_df = input_df.loc[input_df['Unnamed: 0'] == self.simulated_statistic]
            # get the number of the model from the file name
            input_df['model_id'] = model_file.replace(self.indir,'').replace(self.base_file_name,'')
            df = df.append(input_df)
        self.combined_model_results = df

    # write combined results dataframe to output file path
    def write_results(self):
        self.combined_model_results.to_csv(self.outfile,index=False)

def main():
    parser = argparse.ArgumentParser(description='process wiki data')
    parser.add_argument('-i','--indir',
                        required=True,
                        help='path to a coefficient .tsv generated by STATA listcoef')
    parser.add_argument('-o','--outfile',
                        help='path to combined .csv file of model statistics')
    #parser.add_argument('-l','--coef_labels',
    #                    help='path to a coefficient .tsv generated by STATA listcoef')
    parser.add_argument('-s','--simulated_statistic',
                        help='the statistic to simulate')
    parser.add_argument('-f','--base_file_name',
                        default='talk_article_lang_model.tsv',
                        help='the base file name (excluding simulation number) for files to read')
    parser.add_argument('-r','--file_ID_range',
                        nargs=2,
                        type=int,
                        help='specify a range of id numbers for the file to fall between')
    args = parser.parse_args()
    model_reader = ModelReader(indir=args.indir,
                               outfile=args.outfile,
                               simulated_statistic=args.simulated_statistic,
                               base_file_name=args.base_file_name,
                               file_id_range=args.file_ID_range)
    model_reader.get_model_file_list()
    #model_reader.read_model_files()
    #model_reader.write_results()
    
if __name__ == "__main__":
    main()

