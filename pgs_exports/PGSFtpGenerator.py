import sys, os, glob
import re
import shutil
import tarfile
from pgs_exports.PGSBuildFtp import PGSBuildFtp, PGSBuildFtpRemote


#------------------------#
# Class PGSdFtpGenerator #
#------------------------#

class PGSFtpGenerator:
    ''' Generate the PGS FTP structure with metadata files. '''

    def __init__(self,dirpath,dirpath_new,scores_id_list,large_publication_ids_list,previous_release,use_remote_ftp,debug):
        '''
        > Variables:
            - dirpath: path to the directory where the metadata files will be stored
            - dirpath_new: path to the directory where the metadata files will be copied
            - score_ids_list: list of the PGS IDs
            - large_publication_ids_list: list of the PGP IDs that require specific metadata files (large studies)
            - previous_release: date of the previous release
            - use_remote_ftp: flag to indicate if the FTP can be accessed locally of via FTP protocol
            - debug: parameter to test the script (default:0 => non debug mode)
        '''
        self.dirpath = dirpath
        self.dirpath_new = dirpath_new
        self.scores_id_list = scores_id_list
        self.large_publication_ids_list = large_publication_ids_list
        self.previous_release = previous_release
        self.use_remote_ftp = use_remote_ftp
        self.debug = debug
        self.scores_file = dirpath_new+'/pgs_scores_list.txt'



    #=====================#
    #  Copy export files  #
    #=====================#

    def build_metadata_ftp(self):
        ''' Generates PGS specific metadata files (PGS by PGS) '''
        print("\t- Generates PGS specific metadata files (PGS by PGS)")
        temp_data_dir = self.dirpath
        temp_ftp_dir  = self.dirpath_new+'/scores/'

        # Prepare the temporary FTP directory to copy/download all the PGS Scores
        self.create_pgs_directory(self.dirpath_new)
        self.create_pgs_directory(temp_ftp_dir)

        # Create temporary archive directory
        tmp_archive = self.dirpath+'/pgs_archives/'
        if os.path.isdir(tmp_archive):
            shutil.rmtree(tmp_archive,ignore_errors=True)
        self.create_pgs_directory(tmp_archive)

        # 1 - Add metadata for each PGS Score
        for pgs_id in self.scores_id_list:

            # For test only
            if self.debug:
                p = re.compile(r'PGS0+(\d+)$')
                m = p.match(pgs_id)
                pgs_num = m.group(1)
                if pgs_num and int(pgs_num) > self.debug:
                    break

            file_suffix = '_metadata.xlsx'
            if self.use_remote_ftp:
                pgs_ftp = PGSBuildFtpRemote(pgs_id, file_suffix, 'metadata')
            else:
                pgs_ftp = PGSBuildFtp(pgs_id, file_suffix, 'metadata')

            targz_ext = pgs_ftp.meta_file_extension
            meta_file_tar = pgs_id+'_metadata'+targz_ext
            meta_file_xls = pgs_id+file_suffix

            # Build temporary FTP structure for the PGS Metadata
            pgs_main_dir = temp_ftp_dir+pgs_id
            self.create_pgs_directory(pgs_main_dir)
            meta_file_dir = pgs_main_dir+'/Metadata/'
            self.create_pgs_directory(meta_file_dir)

            temp_meta_dir = temp_data_dir+"/"+pgs_ftp.pgs_id+"/Metadata/"

            # 2 - Compare metadata files
            new_file_md5_checksum = pgs_ftp.get_md5_checksum(temp_meta_dir+meta_file_xls)
            ftp_file_md5_checksum = pgs_ftp.get_ftp_md5_checksum()

            # 2 a) - New published Score (PGS directory doesn't exist)
            if not ftp_file_md5_checksum:
                # Copy new files
                shutil.copy2(temp_meta_dir+meta_file_xls, meta_file_dir+meta_file_xls)
                shutil.copy2(temp_data_dir+meta_file_tar, meta_file_dir+meta_file_tar)
                for file in glob.glob(temp_meta_dir+'*.csv'):
                    csv_filepath = file.split('/')
                    filename = csv_filepath[-1]
                    shutil.copy2(file, meta_file_dir+filename)

            # 2 b) - PGS directory exist (Updated Metadata)
            elif new_file_md5_checksum != ftp_file_md5_checksum:
                # Fetch and Copy tar file from FTP
                meta_archives_path = tmp_archive+pgs_id+'_metadata'
                meta_archives_file_tar = pgs_id+'_metadata_'+self.previous_release+targz_ext
                meta_archives_file = tmp_archive+'/'+meta_archives_file_tar
                # Fetch and Copy tar file to the archive
                pgs_ftp.get_ftp_file(meta_file_tar,meta_archives_file)

                if meta_archives_file.endswith(targz_ext):
                    tar = tarfile.open(meta_archives_file, 'r')
                    tar.extractall(meta_archives_path)
                    tar.close()
                else:
                    print("Error: can't extract the file '"+meta_archives_file+"'!")
                    exit(1)

                # Copy CSV files and compare them with the FTP ones
                has_difference = False
                for csv_file in glob.glob(temp_meta_dir+'*.csv'):
                    csv_filepath = csv_file.split('/')
                    filename = csv_filepath[-1]
                    # Copy CSV file to the metadata directory
                    shutil.copy2(csv_file, meta_file_dir+filename)

                    # Compare CSV files
                    ftp_csv_file = meta_archives_path+'/'+filename
                    if os.path.exists(ftp_csv_file):
                        new_csv = pgs_ftp.get_md5_checksum(csv_file)
                        ftp_csv = pgs_ftp.get_md5_checksum(ftp_csv_file)
                        if new_csv != ftp_csv:
                            has_difference = True
                    else:
                        has_difference = True

                # Copy other new files
                shutil.copy2(temp_meta_dir+meta_file_xls, meta_file_dir+meta_file_xls)
                shutil.copy2(temp_data_dir+meta_file_tar, meta_file_dir+meta_file_tar)

                # Archive metadata from previous release
                if has_difference:
                    meta_archives = meta_file_dir+'archived_versions/'
                    self.create_pgs_directory(meta_archives)
                    # Copy tar file to the archive
                    shutil.copy2(meta_archives_file, meta_archives+meta_archives_file_tar)


    def build_bulk_metadata_ftp(self):
        ''' Generates the global metadata files (the ones containing all the PGS metadata) '''
        print("\t- Generates the global metadata files (the ones containing all the PGS metadata)")

        temp_data_dir = self.dirpath
        temp_ftp_dir = self.dirpath_new+'/metadata/'

        # Prepare the temporary FTP directory to copy/download all the PGS Scores
        self.create_pgs_directory(self.dirpath_new)
        self.create_pgs_directory(temp_ftp_dir)

        if self.use_remote_ftp:
            pgs_ftp = PGSBuildFtpRemote('all', '', 'metadata')
        else:
            pgs_ftp = PGSBuildFtp('all', '', 'metadata')

        targz_ext = pgs_ftp.meta_file_extension
        meta_file = pgs_ftp.all_meta_file
        meta_file_xls = meta_file.replace(targz_ext, '.xlsx')

        # Copy new metadata
        shutil.copy2(temp_data_dir+meta_file, temp_ftp_dir+meta_file)
        shutil.copy2(temp_data_dir+'all_metadata/'+meta_file_xls, temp_ftp_dir+meta_file_xls)

        for file in glob.glob(temp_data_dir+'all_metadata/*.csv'):
            csv_filepath = file.split('/')
            filename = csv_filepath[-1]
            shutil.copy2(file, temp_ftp_dir+filename)

        # Archiving metadata from previous release
        meta_archives_file = meta_file.replace(targz_ext, '_'+self.previous_release+targz_ext)

        meta_archives_dir = temp_ftp_dir+'previous_releases/'
        self.create_pgs_directory(meta_archives_dir)

        previous_release_date = self.previous_release.split('-')
        meta_year_archives_dir = meta_archives_dir+previous_release_date[0]+'/'
        self.create_pgs_directory(meta_year_archives_dir)

        pgs_ftp.get_ftp_file(meta_file,meta_year_archives_dir+meta_archives_file)


    def build_large_study_metadata_ftp(self):
        ''' Generates the large study metadata files (the ones containing the PGS metadata for the large studies) '''
        print("\t- Generates the large study metadata files (the ones containing the PGS metadata for the large studies)")

        temp_data_dir = self.dirpath+'/publications_metadata/'
        temp_ftp_dir = self.dirpath_new+'/metadata/publications/'

        # Prepare the temporary FTP directory to copy/download all the PGS Scores
        self.create_pgs_directory(temp_ftp_dir)

        # Create temporary archive directory
        tmp_archive = self.dirpath+'/publication_archives/'
        if os.path.isdir(tmp_archive):
            shutil.rmtree(tmp_archive,ignore_errors=True)
        self.create_pgs_directory(tmp_archive)

        # 1 - Add metadata for each PGS Study
        for pgp_id in self.large_publication_ids_list:

            # For test only
            if self.debug:
                p = re.compile(r'PGP0+(\d+)$')
                m = p.match(pgp_id)
                pgp_num = m.group(1)
                if pgp_num and int(pgp_num) > self.debug:
                    break

            file_suffix = '_metadata.xlsx'
            if self.use_remote_ftp:
                pgs_ftp = PGSBuildFtpRemote(pgp_id, file_suffix, 'publication')
            else:
                pgs_ftp = PGSBuildFtp(pgp_id, file_suffix, 'publication')

            targz_ext = pgs_ftp.meta_file_extension
            meta_file_tar = pgp_id+'_metadata'+targz_ext
            meta_file_xls = meta_file_tar.replace(targz_ext, '.xlsx')

            # Build temporary FTP structure for the PGS Metadata
            pgp_ftp_dir = temp_ftp_dir+pgp_id+'/'
            self.create_pgs_directory(pgp_ftp_dir)

            temp_meta_dir = temp_data_dir+'/'+pgp_id+'/'

            # 2 - Compare metadata files
            new_file_md5_checksum = pgs_ftp.get_md5_checksum(temp_meta_dir+meta_file_xls)
            ftp_file_md5_checksum = pgs_ftp.get_ftp_md5_checksum()

            # 2 a) - New large publication (PGP directory doesn't exist)
            if not ftp_file_md5_checksum:
                # Copy new files
                shutil.copy2(temp_meta_dir+meta_file_xls, pgp_ftp_dir+meta_file_xls)
                shutil.copy2(temp_data_dir+meta_file_tar, pgp_ftp_dir+meta_file_tar)
                for file in glob.glob(temp_meta_dir+'*.csv'):
                    csv_filepath = file.split('/')
                    filename = csv_filepath[-1]
                    shutil.copy2(file, pgp_ftp_dir+filename)

            # 2 b) - PGP directory exist (Updated Metadata)
            elif new_file_md5_checksum != ftp_file_md5_checksum:
                # Fetch and Copy tar file from FTP
                meta_archives_path = tmp_archive+'/'+pgp_id+'_metadata'
                meta_archives_file_tar = pgp_id+'_metadata_'+self.previous_release+targz_ext
                meta_archives_file = tmp_archive+'/'+meta_archives_file_tar
                # Fetch and Copy tar file to the archive
                pgs_ftp.get_ftp_file(meta_file_tar,meta_archives_file)

                if meta_archives_file.endswith(targz_ext):
                    tar = tarfile.open(meta_archives_file, 'r')
                    tar.extractall(meta_archives_path)
                    tar.close()
                else:
                    print("Error: can't extract the file '"+meta_archives_file+"'!")
                    exit(1)

                # Copy CSV files and compare them with the FTP ones
                has_difference = False
                for csv_file in glob.glob(temp_meta_dir+'*.csv'):
                    csv_filepath = csv_file.split('/')
                    filename = csv_filepath[-1]
                    # Copy CSV file to the metadata directory
                    shutil.copy2(csv_file, pgp_ftp_dir+filename)

                    # Compare CSV files
                    ftp_csv_file = meta_archives_path+'/'+filename
                    if os.path.exists(ftp_csv_file):
                        new_csv = pgs_ftp.get_md5_checksum(csv_file)
                        ftp_csv = pgs_ftp.get_md5_checksum(ftp_csv_file)
                        if new_csv != ftp_csv:
                            has_difference = True
                    else:
                        has_difference = True

                # Copy other new files
                shutil.copy2(temp_meta_dir+meta_file_xls, pgp_ftp_dir+meta_file_xls)
                shutil.copy2(temp_data_dir+meta_file_tar, pgp_ftp_dir+meta_file_tar)

                # Archive metadata from previous release
                if has_difference:
                    meta_archives = pgp_ftp_dir+'archived_versions/'
                    self.create_pgs_directory(meta_archives)
                    # Copy tar file to the archive
                    shutil.copy2(meta_archives_file, meta_archives+meta_archives_file_tar)


    def create_pgs_directory(self, path, force_recreate=None):
        '''
        Creates directory for a given PGS
        > Parameters:
            - path: path of the directory
            - force_recreate: if it already exists, remove it before creating it again
        '''
        # Remove directory before creating it again
        if force_recreate and os.path.isdir(path):
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