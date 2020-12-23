import sys, os.path
from pgs_exports.PGSExport import PGSExport, PGSExportAllMetadata

#--------------------------------#
# Class class PGSExportGenerator #
#--------------------------------#

class PGSExportGenerator:
    """ Generates the different PGS exports. """

    def __init__(self,dirpath,data,scores_file,score_ids_list,latest_release,debug):
        """
        > Variables:
            - dirpath: path to the directory where the metadata files will be stored
            - data: dictionary containing the metadata
            - scores_file: path to the file where we want to write the list of PGS IDs
            - score_ids_list: list of the PGS IDs
            - latest_release: date of the latest (i.e. new) release
            - debug: parameter to test the script (default:0 => non debug mode)
            - scores_file: path to the file where we want to write the list of PGS IDs
        """
        self.dirpath = dirpath
        self.data = data
        self.scores_file = scores_file 
        self.score_ids_list = score_ids_list
        self.latest_release = latest_release
        self.debug = debug


    def generate_scores_list_file(self):
        """ Generate file listing all the released Scores """
        print("\t- Generate file listing all the released Scores")
        
        file = open(self.scores_file, 'w')
        for score in self.score_ids_list:
            file.write(score+'\n')
        file.close()


    def call_generate_all_metadata_exports(self):
        """ Generate all PGS metadata export files """
        print("\t- Generate all PGS metadata export files")

        datadir = self.dirpath+"all_metadata/"
        filename = datadir+'pgs_all_metadata.xlsx'

        csv_prefix = datadir+'pgs_all'

        if not os.path.isdir(datadir):
            try:
                os.mkdir(datadir)
            except OSError:
                print (f'Creation of the directory {datadir} failed')

        if not os.path.isdir(datadir):
            print(f'Can\'t create a directory for the metadata ({datadir})')
            exit(1)

        # Create export object
        pgs_export = PGSExportAllMetadata(filename, self.data)

        if self.debug:
            pgs_ids_list = []
            for i in range(1,self.debug+1):
                num = i < 10 and '0'+str(i) or str(i)
                pgs_ids_list.append('PGS0000'+num)
            pgs_export.set_pgs_list(pgs_ids_list)

        # Info/readme spreadsheet
        pgs_export.create_readme_spreadsheet(self.latest_release)

        # Build the spreadsheets
        pgs_export.generate_sheets(csv_prefix)

        # Close the Pandas Excel writer and output the Excel file.
        pgs_export.save()

        # Create a md5 checksum for the spreadsheet
        pgs_export.create_md5_checksum()

        # Generate a tar file of the study data
        pgs_export.generate_tarfile(self.dirpath+"pgs_all_metadata.tar.gz",datadir)


    def call_generate_studies_metadata_exports(self):
        """ Generate PGS metadata export files for each released studies """
        print("\t- Generate PGS metadata export files for each released studies")

        if self.debug:
            pgs_ids_list = []
            for i in range(1,self.debug+1):
                num = i < 10 and '0'+str(i) or str(i)
                pgs_ids_list.append('PGS0000'+num)
        else:
            pgs_ids_list = [  x['id'] for x in self.data['score'] ]

        # Loop over the PGS IDs
        for pgs_id in pgs_ids_list:

            print("\n# PGS "+pgs_id)

            pgs_dir = self.dirpath+pgs_id
            study_dir = pgs_dir+"/Metadata/"
            csv_prefix = study_dir+pgs_id

            # Check / create PGS directory
            if not os.path.isdir(pgs_dir):
                try:
                    os.mkdir(pgs_dir)
                except OSError:
                    print ("Creation of the directory %s failed" % pgs_dir)

            # Check / create PGS metadata directory
            if os.path.isdir(pgs_dir) and not os.path.isdir(study_dir):
                try:
                    os.mkdir(study_dir)
                except OSError:
                    print ("Creation of the directory %s failed" % study_dir)

            if not os.path.isdir(study_dir):
                print("Can't create a directory for the study "+pgs_id)
                break

            filename = study_dir+pgs_id+"_metadata.xlsx"

            print("FILENAME: "+filename)

            # Create export object
            pgs_export = PGSExport(filename, self.data)
            pgs_export.set_pgs_list([pgs_id])

            # Build the spreadsheets
            pgs_export.generate_sheets(csv_prefix)

            # Close the Pandas Excel writer and output the Excel file.
            pgs_export.save()

            # Generate a tar file of the study data
            pgs_export.generate_tarfile(self.dirpath+pgs_id+"_metadata.tar.gz",study_dir)
