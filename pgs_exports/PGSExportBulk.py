import sys, os.path
import pandas as pd
from pgs_exports.PGSExport import PGSExport


#---------------------#
# Spreadsheet methods #
#---------------------#

class PGSExportAllMetadata(PGSExport):

    def create_readme_spreadsheet(self, release):
        """ Info/readme spreadsheet """

        readme_data = {}

        readme_data['PGS Catalog version'] = [release]
        readme_data['Number of Polygenic Scores'] = [len(self.data['score'])]
        readme_data['Number of Traits'] = [len(self.data['trait'])]
        readme_data['Number of Publications'] = [len(self.data['publication'])]

        df = pd.DataFrame(readme_data)
        df = df.transpose()
        df.to_excel(self.writer, sheet_name="Readme", header=False)