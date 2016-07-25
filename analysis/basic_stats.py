import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import argparse
import config
import pandas as pd
import utils

class ML_WP_Analyzer(object):

    def __init__(self,infile,analysis_unit,namespace,outfile=None):
        # raw dataframe
        self.df = pd.read_csv(infile,
                              na_values={'title':''},
                              keep_default_na=False,
                              dtype={'title': object})
        self.namespace = namespace
        # dataframe of basic stats
        self.result = None
        # unit of analysis (edits or editors)
        if analysis_unit == 'edits':
            self.analysis_unit = 'len_{0}'.format(self.namespace)
        elif analysis_unit == 'edits':
            self.analysis_unit = 'num_editors_{0}'.format(self.namespace)
        else:
            utils.log('invalid unit of analysis!')
            sys.exit(0)
        # path to write .csv of results, if
        if outfile == 'default':
            self.outfile = self._format_outfile_name(outfile)
        else:
            self.outfile = outfile
        
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
        if self.outfile:
            self.result.to_csv(self.outfile,encoding='utf-8',index=False)
        else:
            utils.log('No outfile specified!')

    def _format_outfile_name(self,outfile):
        outfile = os.path.join(config.RESULTS_DIR,config.BASIC_STATS)
        outfile = outfile.replace('.csv','_{0}_{1}.csv'.format(self.analysis_unit,self.namespace))
        return outfile

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
                        default=os.path.join(config.ROOT_PROCESSED_DIR,'combined',config.COMBINED_EDIT_RATIOS),
                        help='csv of all language edits')
    parser.add_argument('-o','--outfile',
                        default='default',
                        help='output file path for results')
    parser.add_argument('-u','--analysis_unit',
                        default='edits',
                        choices=['edits','editors'],
                        help='unit for analysis (edits or editors)')
    parser.add_argument('-n','--namespace',
                        default=1,
                        type=int,
                        choices=[0,1],
                        help='namespace for analysis (0 or 1)')
    args = parser.parse_args()
    a = ML_WP_Analyzer(infile=args.infile,
                       outfile=args.outfile,
                       analysis_unit=args.analysis_unit,
                       namespace=args.namespace)
    a.generate_stats()
    a.write_csv()

if __name__ == "__main__":
    main()
