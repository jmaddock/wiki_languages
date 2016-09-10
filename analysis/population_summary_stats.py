import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import argparse
import config
import pandas as pd
import utils

class ML_WP_Population_Analyzer(object):

    def __init__(self,infile,outfile=None,time_variable='years'):
        # raw dataframe
        df = pd.read_csv(
            infile,
            na_values={'title':''},
            keep_default_na=False,
            dtype={'title': object}
        )
        self.df = self._transform_page_age(df,time_variable)
        self.independent_vars = [
            'len_1',
            'len_0',
            'tds_1',
            'tds_0',
            'num_editors_1',
            'num_editors_0',
        ]
        self.variable_names = {
            'len_1':'num_talk_edits',
            'len_0':'num_article_edits',
            'tds_1':'talk_age',
            'tds_0':'article_age',
            'num_editors_1':'unique_talk_authors',
            'num_editors_0':'unique_article_authors',
        }
        # dataframe of basic stats
        self.result = None
        # path to write .csv of results
        self.outfile = outfile

    def _transform_page_age(self,df,time_variable):
        if time_variable == 'years':
            divide_by = 60*60*24*365
            df['tds_0'] = df['tds_0'].divide(divide_by)
            df['tds_1'] = df['tds_1'].divide(divide_by)
        return df
        
    def generate_stats(self):
        # construct the "index" from unique languages
        result = pd.DataFrame({'independent_variable':self.independent_vars})
        # find the totals for each language
        result = result.merge(self._get_totals(),on='independent_variable')
        # find the means for each language
        result = result.merge(self._get_means(),on='independent_variable')
        # find the medians for each language
        result = result.merge(self._get_medians(),on='independent_variable')
        # find the std for each language
        result = result.merge(self._get_std(),on='independent_variable')
        # find the variance for each language
        result = result.merge(self._get_var(),on='independent_variable')
        result = result.replace(to_replace={'independent_variable':self.variable_names})
        print(result)
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
        by_lang = self.df[self.independent_vars].sum().to_frame('total').reset_index().rename(columns={'index':'independent_variable'})
        print(by_lang)
        return by_lang

    def _get_means(self):
        utils.log('calculating means')
        by_lang = self.df[self.independent_vars].mean().to_frame('mean').reset_index().rename(columns={'index':'independent_variable'})
        return by_lang
        
    def _get_medians(self):
        utils.log('calculating medians')
        by_lang = self.df[self.independent_vars].median().to_frame('median').reset_index().rename(columns={'index':'independent_variable'})
        return by_lang

    def _get_std(self):
        utils.log('calculating standard deviations')
        by_lang = self.df[self.independent_vars].std().to_frame('std').reset_index().rename(columns={'index':'independent_variable'})
        return by_lang

    def _get_var(self):
        utils.log('calculating variance')
        by_lang = self.df[self.independent_vars].var().to_frame('var').reset_index().rename(columns={'index':'independent_variable'})
        return by_lang
        
def main():
    parser = argparse.ArgumentParser(description='generate sheet of basic stats for each language')
    parser.add_argument('-i','--infile',
                        default=os.path.join(config.ROOT_PROCESSED_DIR,'combined',config.COMBINED_EDIT_RATIOS),
                        help='csv of all language edits')
    parser.add_argument('-o','--outfile',
                        default='default',
                        help='output file path for results')
    parser.add_argument('-t','--time_variable',
                        choices=['seconds','years'],
                        default='years',
                        help='the time increment to use for page age')
    args = parser.parse_args()
    a = ML_WP_Population_Analyzer(infile=args.infile,
                                  outfile=args.outfile,
                                  time_variable=args.time_variable)
    a.generate_stats()
    a.write_csv()

if __name__ == "__main__":
    main()
