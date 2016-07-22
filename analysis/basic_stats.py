import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import argparse
import config
import pandas as pd
import utils

class ML_WP_Analyzer(object):

    def __init__(self,infile,analysis_unit,outfile=None):
        # raw dataframe
        self.df = pd.read_csv(infile,
                              na_values={'title':''},
                              keep_default_na=False,
                              dtype={'title': object})
        # path to write .csv of results
        self.outfile = outfile
        # dataframe of basic stats
        self.result = None
        # unit of analysis (edits or editors)
        if analysis_unit == 'edits':
            self.analysis_unit = 'len'
        elif analysis_unit == 'edits':
            self.analysis_unit = 'num_editors'
        else:
            utils.log('invalid unit of analysis!')
            sys.exit(0)

    def generate_stats(self):
        # construct the "index" from unique languages
        result = pd.DataFrame({'lang':self.df['lang'].unique()})
        # find the totals for each language
        result = result.merge(self._get_totals(),on='lang')
        # find the means for each language
        result = result.merge(self._get_means(),on='lang')
        # find the medians for each language
        result = result.merge(self._get_medians(),on='lang')
        # find the std for each language
        result = result.merge(self._get_std(),on='lang')
        # find the variance for each language
        result = result.merge(self._get_var(),on='lang')
        self.result = result
        return result

    def write_csv(self):
        self.result.to_csv(self.outfile,encoding='utf-8',index=False)

    def _get_totals(self):
        utils.log('calculating totals')
        return self.df.groupby('lang')[self.analysis_unit].sum().to_frame('total').reset_index()

    def _get_means(self):
        utils.log('calculating means')
        return self.df.groupby('lang')[self.analysis_unit].mean().to_frame('mean').reset_index()

    def _get_medians(self):
        utils.log('calculating medians')
        return self.df.groupby('lang')[self.analysis_unit].median().to_frame('median').reset_index()

    def _get_std(self):
        utils.log('calculating standard deviations')
        return self.df.groupby('lang')[self.analysis_unit].std().to_frame('std').reset_index()

    def _get_var(self):
        utils.log('calculating variance')
        return self.df.groupby('lang')[self.analysis_unit].var().to_frame('var').reset_index()
        
def main():
    parser = argparse.ArgumentParser(description='generate sheet of basic stats for each language')
    parser.add_argument('-i','--infile',
                        default=config.COMBINED_EDIT_RATIOS,
                        help='csv of all language edits')
    parser.add_argument('-o','--outfile',
                        default=os.path.join(config.RESULTS_DIR,config.BASIC_STATS),
                        help='output file path for results')
    parser.add_argument('-u','--analysis_unit',
                        default='edits',
                        help='unit for analysis (edits or editors)')
    args = parser.parse_args()
    a = ML_WP_Analyzer(infile=args.infile,
                       outfile=args.outfile,
                       analysis_unit=args.analysis_unit)
    a.generate_stats()
    a.write_csv()

if __name__ == "__main__":
    main()
