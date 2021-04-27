import os.path, shutil
import unittest
import json
import hashlib
from pgs_exports.PGSExportGenerator import PGSExportGenerator
from pgs_exports.PGSBuildFtp import PGSBuildFtp


class TestSum(unittest.TestCase):

    current_dir = os.path.abspath(__file__).split(__file__)[0]

    csv_files_types = [
        'efo_traits', 
        'evaluation_sample_sets', 
        'performance_metrics', 
        'publications',
        'score_development_samples',
        'scores'
    ]

    ancestry_categories = {
        "MAE": "Multi-ancestry (including European)",
        "MAO": "Multi-ancestry (excluding European)",
        "AFR": "African",
        "EAS": "East Asian",
        "SAS": "South Asian",
        "ASN": "Additional Asian Ancestries",
        "EUR": "European",
        "GME": "Greater Middle Eastern",
        "AMR": "Hispanic or Latin American",
        "OTH": "Additional Diverse Ancestries",
        "NR": "Not Reported"
    }
    
    export_dir = current_dir+'/tests/export/'
    scores_list_file = export_dir+'pgs_scores_list.txt'
    current_release_date = '2020-12-15'
    debug = False
    input_data_dir = current_dir+'/tests/data'
    if not os.path.isdir(input_data_dir):
        print(f'Error: can\'t find the directory {input_data_dir}')
        exit(1)


    def get_all_data(self):
        self.data = {}
        for type in ['score', 'trait', 'publication', 'performance']:
            json_file_path = self.input_data_dir+'/'+type+'.json'
            if not os.path.isfile(json_file_path):
                print(f'Error: can\'t find the file {json_file_path}')
                exit(1)
            with open(json_file_path) as f:
                json_data = json.load(f)
            self.data[type] = json_data


    def generates_export_files(self):

        self.create_pgs_directory(self.export_dir)


        # Get the list of published PGS IDs
        self.score_ids_list = [ x['id'] for x in self.data['score'] ]

        exports_generator = PGSExportGenerator(self.export_dir,self.data,self.scores_list_file,self.score_ids_list,self.current_release_date,self.ancestry_categories,self.debug)

        # Generate file listing all the released Scores
        exports_generator.generate_scores_list_file()

        # Generate all PGS metadata export files
        exports_generator.call_generate_all_metadata_exports()

        # Generate PGS metadata export files for each released studies
        exports_generator.call_generate_studies_metadata_exports()


    def create_pgs_directory(self,path):
        """
        Creates directory for a given PGS
        > Parameters:
            - path: path of the directory
        """
        # Remove directory before creating it again
        if os.path.isdir(path):
            try:
                shutil.rmtree(path,ignore_errors=True)
            except OSError:
                print (f'Deletion of the existing directory prior to it\'s regeneration failed ({path}).')
                exit()

        # Create directory if it doesn't exist
        if not os.path.isdir(path):
            try:
                os.mkdir(path, 0o755)
            except OSError:
                print (f'Creation of the directory {path} failed')
                exit()

    
    def compare_files(self):
        """ Compare Scores list and CSV files, and check that the Excel and tar.gz files exist and are not null """

        # PGS scores list
        print("# Scores list file")
        score_filename = os.path.basename(self.scores_list_file)
        ref_scores_list_filepath = f'{self.current_dir}/tests/output/{score_filename}'
        self.compare_md5_and_size(score_filename,self.scores_list_file,ref_scores_list_filepath)

        pgs_all = 'pgs_all'
        # Compare individual files
        for pgs_id in (*self.score_ids_list, pgs_all):
            print("# "+pgs_id)
            if pgs_id == pgs_all:
                test_dir = f'{self.export_dir}/all_metadata'
                ref_dir = f'{self.current_dir}/tests/output/all_metadata'
            else:
                test_dir = f'{self.export_dir}/{pgs_id}/Metadata'
                ref_dir = f'{self.current_dir}/tests/output/{pgs_id}/Metadata'
            # CSV files
            for type in self.csv_files_types:
                filename = f'{pgs_id}_metadata_{type}.csv'
                test_filepath = f'{test_dir}/{filename}'
                ref_filepath = f'{ref_dir}/{filename}'
                self.compare_md5_and_size(filename,test_filepath,ref_filepath)

            # Excel file
            xls_filename = f'{pgs_id}_metadata.xlsx'
            test_xls_filepath = f'{test_dir}/{xls_filename}'
            # Test file exist
            self.assertEqual(os.path.exists(test_xls_filepath),1)
            # Test file not empty
            test_xls_filesize = os.path.getsize(test_xls_filepath)
            self.assertGreater(test_xls_filesize,0)

            # tar.gz file
            tar_filename = f'{pgs_id}_metadata.tar.gz'
            test_tar_filepath = f'{self.export_dir}/{tar_filename}'
            # Test file exist
            self.assertEqual(os.path.exists(test_tar_filepath),1)
            # Test file not empty
            test_tar_filesize = os.path.getsize(test_tar_filepath)
            self.assertGreater(test_tar_filesize,0)


    def get_md5_file_checksum(self,filename,blocksize=4096):
        """ Returns MD5 checksum for the given file. """

        md5 = hashlib.md5()
        try:
            file = open(filename, 'rb')
            with file:
                for block in iter(lambda: file.read(blocksize), b""):
                    md5.update(block)
        except IOError:
            print('File \'' + filename + '\' not found!')
            return None
        except:
            print("Error: the script couldn't generate a MD5 checksum for '" + filename + "'!")
            return None

        return md5.hexdigest()


    def compare_md5_and_size(self,filename,test_filepath,ref_filepath):
        print(f' - {filename}')
        # Test file exist
        self.assertEqual(os.path.exists(test_filepath),1)

        # Compare file MD5
        test_file_md5 = self.get_md5_file_checksum(test_filepath)
        ref_file_md5 = self.get_md5_file_checksum(ref_filepath)
        try:
            self.assertEqual(test_file_md5,ref_file_md5)
        except:
            print(f'\t> MD5: test => {test_file_md5}')
            print(f'\t> MD5: ref  => {ref_file_md5}')
            # Compare file sizes
            test_filesize = os.path.getsize(test_filepath)
            ref_filesize = os.path.getsize(ref_filepath)
            if test_filesize != ref_filesize:
                print(f'\t> Size: test => {test_filesize}')
                print(f'\t> Size: ref  => {ref_filesize}')
            self.assertEqual(test_filesize,ref_filesize)



if __name__ == "__main__":
    export_test = TestSum()
    export_test.get_all_data()
    export_test.generates_export_files()
    export_test.compare_files()