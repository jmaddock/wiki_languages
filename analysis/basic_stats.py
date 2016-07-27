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
        # wiki namespace (0 or 1)
        if (namespace == None and (analysis_unit in ['edit_ratios','editor_ratios'])) or namespace in [0,1]:
            self.namespace = namespace
        else:
            utils.log('invalid namespace and unit of analysis combination!')
            sys.exit(0)
        # dataframe of basic stats
        self.result = None
        # unit of analysis (edits or editors)
        if analysis_unit == 'edits':
            self.analysis_unit = 'len_{0}'.format(self.namespace)
        elif analysis_unit == 'editors':
            self.analysis_unit = 'num_editors_{0}'.format(self.namespace)
        elif analysis_unit == 'edit_ratios':
            self.analysis_unit = 'ratio'
        elif analysis_unit == 'editor_ratios':
            self.analysis_unit = 'editor_ratio'
        else:
            utils.log('invalid unit of analysis!')
            sys.exit(0)
        # path to write .csv of results
        # if outfile = 'default', create path from config file
        if outfile == 'default':
            self.outfile = self._format_outfile_name(outfile)
        else:
            self.outfile = outfile
        
    def generate_stats(self):
        # construct the "index" from unique languages
        result = pd.DataFrame({'lang':self.df['lang'].unique()})
        result = result.append(pd.DataFrame([{'lang':'all'}]))
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
        outfile = outfile.replace('.csv','_{0}.csv'.format(self.analysis_unit))
        return outfile

    def _get_totals(self):
        utils.log('calculating totals')
        by_lang = self.df.groupby('lang')[self.analysis_unit].sum().to_frame('total').reset_index()
        total = pd.DataFrame([{'lang':'all','total':self.df[self.analysis_unit].sum()}])
        return by_lang.append(total).reset_index(drop=True)

    def _get_means(self):
        utils.log('calculating means')
        by_lang = self.df.groupby('lang')[self.analysis_unit].mean().to_frame('mean').reset_index()
        total = pd.DataFrame([{'lang':'all','mean':self.df[self.analysis_unit].mean()}])
        return by_lang.append(total).reset_index(drop=True)
        
    def _get_medians(self):
        utils.log('calculating medians')
        by_lang = self.df.groupby('lang')[self.analysis_unit].median().to_frame('median').reset_index()
        total = pd.DataFrame([{'lang':'all','median':self.df[self.analysis_unit].median()}])
        return by_lang.append(total).reset_index(drop=True)

    def _get_std(self):
        utils.log('calculating standard deviations')
        by_lang = self.df.groupby('lang')[self.analysis_unit].std().to_frame('std').reset_index()
        total = pd.DataFrame([{'lang':'all','std':self.df[self.analysis_unit].std()}])
        return by_lang.append(total).reset_index(drop=True)

    def _get_var(self):
        utils.log('calculating variance')
        by_lang = self.df.groupby('lang')[self.analysis_unit].var().to_frame('var').reset_index()
        total = pd.DataFrame([{'lang':'all','var':self.df[self.analysis_unit].var()}])
        return by_lang.append(total).reset_index(drop=True)
        
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
                        choices=['edits','editors','edit_ratios','editor_ratios'],
                        help='unit for analysis (edits or editors)')
    parser.add_argument('-n','--namespace',
                        default=None,
                        type=int,
                        choices=[0,1,None],
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
