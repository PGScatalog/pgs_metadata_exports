import os, os.path, sys, glob
import re
import argparse
import requests
import shutil
import tarfile
from pgs_exports.PGSExport import PGSExport
from pgs_exports.PGSExportBulk import PGSExportAllMetadata
from pgs_exports.PGSBuildFtp import PGSBuildFtp
from pgs_exports.PGSBuildFtpRemote import PGSBuildFtpRemote


def rest_api_call(url,endpoint,parameters=None):
    if not url.endswith('/'):
        url += '/'
    rest_full_url = url+endpoint
    if parameters:
        rest_full_url += '?'+parameters
    
    print("\t\t> URL: "+rest_full_url)
    try:
        response = requests.get(rest_full_url)
        response_json = response.json()
        # With pagination
        if 'next' in response_json:
            count_items = response_json['count']
            results = response_json['results']
            while response_json['next']:
                response = requests.get(response_json['next'])
                response_json = response.json()
                    
                results = results + response_json['results']
            if count_items != len(results):
                print(f'The number of items are differents from expected: {len(results)} found instead of {count_items}')
        else:
            results = response_json
    except requests.exceptions.RequestException as e:  # This is the correct syntax
        raise SystemExit(e)
    return results


def get_all_pgs_data(url_root):
    """ Fetch all the PGS data via the REST API """
    data = {}
    for type in ['score', 'trait', 'publication', 'performance']:
        print(f'\t- Fetch all {type}s')
        tmp_data = rest_api_call(url_root, f'{type}/all')
        if tmp_data:
            print(f'\t\t> {type}s: {len(tmp_data)} entries')
            data[type] = tmp_data
        else:
            print(f'\t/!\ Error: cannot retrieve "{type}" data')
    return data


def get_latest_release(url_root):
    """ Fetch the date of the latest the PGS Catalog release """
    release = ''
    release_data = rest_api_call(url_root, 'release/current')
    if release_data:
        release = release_data
        print(f'\t\t> Release: {release["date"]}')
    else:
        print('\t/!\ Error: cannot retrieve current release')
    return release


def get_previous_release(url_root):
    """ Fetch the date of the latest the PGS Catalog release """
    release = ''
    release_data = rest_api_call(url_root, 'release/all')
    if release_data:
        if 'results' in release_data:
            release = release_data['results'][1]
        else:
            release = release_data[1]
        print(f'\t\t> Previous release: {release["date"]}')
    else:
        print('\t/!\ Error: cannot retrieve previous release')
    return release


def create_pgs_directory(path, force_recreate=None):
    """ Creates directory for a given PGS """
    # Remove directory before generating it again
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


def tardir(path, tar_name):
    """ Generates a tarball of the new PGS FTP metadata files """
    with tarfile.open(tar_name, "w:gz") as tar_handle:
        for root, dirs, files in os.walk(path):
            for file in files:
                tar_handle.add(os.path.join(root, file))




#=========================#
#  Generate export files  #
#=========================#

def generate_scores_list_file(scores_file, score_ids_list):
    """ Generate file listing all the released Scores """
    print("\t- Generate file listing all the released Scores")
    
    file = open(scores_file, 'w')
    for score in score_ids_list:
        file.write(score+'\n')
    file.close()


def call_generate_all_metadata_exports(dirpath,data,debug,latest_release):
    """ Generate all PGS metadata export files """
    print("\t- Generate all PGS metadata export files")

    datadir = dirpath+"all_metadata/"
    filename = datadir+'pgs_all_metadata.xlsx'

    csv_prefix = datadir+'pgs_all'

    if not os.path.isdir(datadir):
        try:
            os.mkdir(datadir)
        except OSError:
            print ("Creation of the directory %s failed" % datadir)

    if not os.path.isdir(datadir):
        print("Can't create a directory for the metadata ("+datadir+")")
        exit(1)

    # Create export object
    pgs_export = PGSExportAllMetadata(filename, data)

    if debug:
        pgs_ids_list = []
        for i in range(1,debug+1):
            num = i < 10 and '0'+str(i) or str(i)
            pgs_ids_list.append('PGS0000'+num)
        pgs_export.set_pgs_list(pgs_ids_list)

    # Info/readme spreadsheet
    pgs_export.create_readme_spreadsheet(latest_release)

    # Build the spreadsheets
    pgs_export.generate_sheets(csv_prefix)

    # Close the Pandas Excel writer and output the Excel file.
    pgs_export.save()

    # Create a md5 checksum for the spreadsheet
    pgs_export.create_md5_checksum()

    # Generate a tar file of the study data
    pgs_export.generate_tarfile(dirpath+"pgs_all_metadata.tar.gz",datadir)


def call_generate_studies_metadata_exports(dirpath,data,debug):
    """ Generate PGS metadata export files for each released studies """
    print("\t- Generate PGS metadata export files for each released studies")

    if debug:
        pgs_ids_list = []
        for i in range(1,debug+1):
            num = i < 10 and '0'+str(i) or str(i)
            pgs_ids_list.append('PGS0000'+num)
    else:
        pgs_ids_list = [  x['id'] for x in data['score'] ]

    for pgs_id in pgs_ids_list:

        print("\n# PGS "+pgs_id)

        pgs_dir = dirpath+pgs_id
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
        pgs_export = PGSExport(filename, data)
        pgs_export.set_pgs_list([pgs_id])

        # Build the spreadsheets
        pgs_export.generate_sheets(csv_prefix)

        # Close the Pandas Excel writer and output the Excel file.
        pgs_export.save()

        # Generate a tar file of the study data
        pgs_export.generate_tarfile(dirpath+pgs_id+"_metadata.tar.gz",study_dir)


#=====================#
#  Copy export files  #
#=====================#

def build_metadata_ftp(dirpath,dirpath_new,scores_id_list,previous_release,use_remote_ftp,debug):
    """ Generates PGS specific metadata files (PGS by PGS) """
    print("\t- Generates PGS specific metadata files (PGS by PGS)")
    temp_data_dir = dirpath
    temp_ftp_dir  = dirpath_new+'/scores/'

    # Prepare the temporary FTP directory to copy/download all the PGS Scores
    create_pgs_directory(dirpath_new)
    create_pgs_directory(temp_ftp_dir)

    # Create temporary archive directory
    tmp_archive = dirpath+'/pgs_archives/'
    if os.path.isdir(tmp_archive):
        shutil.rmtree(tmp_archive,ignore_errors=True)
    create_pgs_directory(tmp_archive)

    # 1 - Add metadata for each PGS Score
    for pgs_id in scores_id_list:

        # For test only
        if debug:
            p = re.compile(r'PGS0+(\d+)$')
            m = p.match(pgs_id)
            pgs_num = m.group(1)
            if pgs_num and int(pgs_num) > debug:
                break

        file_suffix = '_metadata.xlsx'
        if use_remote_ftp:
            pgs_ftp = PGSBuildFtpRemote(pgs_id, file_suffix, 'metadata')
        else:
            pgs_ftp = PGSBuildFtp(pgs_id, file_suffix, 'metadata')

        meta_file_tar = pgs_id+'_metadata'+pgs_ftp.meta_file_extension
        meta_file_xls = pgs_id+file_suffix

        # Build temporary FTP structure for the PGS Metadata
        pgs_main_dir = temp_ftp_dir+pgs_id
        create_pgs_directory(pgs_main_dir)
        meta_file_dir = pgs_main_dir+'/Metadata/'
        create_pgs_directory(meta_file_dir)

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
            meta_archives_file_tar = pgs_id+'_metadata_'+previous_release+pgs_ftp.meta_file_extension
            meta_archives_file = tmp_archive+'/'+meta_archives_file_tar
            # Fetch and Copy tar file to the archive
            pgs_ftp.get_ftp_file(meta_file_tar,meta_archives_file)

            if meta_archives_file.endswith(pgs_ftp.meta_file_extension):
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
                create_pgs_directory(meta_archives)
                # Copy tar file to the archive
                shutil.copy2(meta_archives_file, meta_archives+meta_archives_file_tar)


def build_bulk_metadata_ftp(dirpath,dirpath_new,previous_release,use_remote_ftp):
    """ Generates the global metadata files (the ones containing all the PGS metadata) """
    print("\t- Generates the global metadata files (the ones containing all the PGS metadata)")

    temp_data_dir = dirpath
    temp_ftp_dir = dirpath_new+'/metadata/'

    # Prepare the temporary FTP directory to copy/download all the PGS Scores
    create_pgs_directory(dirpath_new)
    create_pgs_directory(temp_ftp_dir)

    if use_remote_ftp:
        pgs_ftp = PGSBuildFtpRemote('all', '', 'metadata')
    else:
        pgs_ftp = PGSBuildFtp('all', '', 'metadata')

    meta_file = pgs_ftp.all_meta_file
    meta_file_xls = meta_file.replace('.tar.gz', '.xlsx')

    # Copy new metadata
    shutil.copy2(temp_data_dir+meta_file, temp_ftp_dir+meta_file)
    shutil.copy2(temp_data_dir+'all_metadata/'+meta_file_xls, temp_ftp_dir+meta_file_xls)

    for file in glob.glob(temp_data_dir+'all_metadata/*.csv'):
        csv_filepath = file.split('/')
        filename = csv_filepath[-1]
        shutil.copy2(file, temp_ftp_dir+filename)

    # Archiving metadata from previous release
    meta_archives_file = meta_file.replace('.tar.gz', '_'+previous_release+'.tar.gz')

    meta_archives_dir = temp_ftp_dir+'previous_releases/'
    create_pgs_directory(meta_archives_dir)

    previous_release_date = previous_release.split('-')
    meta_year_archives_dir = meta_archives_dir+previous_release_date[0]+'/'
    create_pgs_directory(meta_year_archives_dir)

    pgs_ftp.get_ftp_file(meta_file,meta_year_archives_dir+meta_archives_file)


def check_new_data_entry_in_metadata(dirpath_new,data,release_data):
    """ Check that the metadata directory for the new Scores and Performance Metrics exists """
    scores_dir = dirpath_new+'/scores/'
 
    # Score(s)
    missing_score_dir = set()
    for score_id in release_data['released_score_ids']:
        if not os.path.isdir(scores_dir+score_id):
            missing_score_dir.add(score_id)
    # Performance Metric(s)
    missing_perf_dir = set()
    new_performances = release_data['released_performance_ids']
    for perf in [ x for x in data['performance'] if x['id'] in new_performances]:
        score_id = perf['associated_pgs_id']
        if not os.path.isdir(scores_dir+score_id):
            missing_perf_dir.add(score_id)

    if len(missing_score_dir) != 0 or len(missing_perf_dir) != 0:
        if len(missing_score_dir) != 0:
            print('/!\ Missing PGS directories for the new entry(ies):\n - '+'\n - '.join(list(missing_score_dir)))
        if len(missing_perf_dir) != 0:
            print('/!\ Missing PGS directories for the new associated Performance Metric entry(ies):\n - '+'\n - '.join(list(missing_perf_dir)))
        exit(1)
    else:
        print("OK - No missing PGS directory for the new  entry(ies)")




#===============#
#  Main method  #
#===============#

def main():

    debug = 0
    tmp_export_dir_name = 'export'
    tmp_ftp_dir_name = 'new_ftp_content'
    argparser = argparse.ArgumentParser()
    argparser.add_argument("--url", help='The URL root of the REST API, e.g. "http://127.0.0.1:8000/rest/"', required=True)
    argparser.add_argument("--dir", help=f'The path of the root dir of the metadata "<dir>/{tmp_ftp_dir_name}"', required=True)
    argparser.add_argument("--remote_ftp", help='Flag to indicate whether the FTP is remote (FTP protocol) or local (file system)', action='store_true')

    args = argparser.parse_args()

    rest_url_root = args.url
    content_dir = args.dir

    use_remote_ftp = False
    if args.remote_ftp:
        use_remote_ftp = True

    if not os.path.isdir(content_dir):
        print(f'Directory {content_dir} can\'t be found!')
        exit(1)

    new_ftp_dir = content_dir+'/'+tmp_ftp_dir_name
    create_pgs_directory(new_ftp_dir , 1)

    export_dir = content_dir+'/'+tmp_export_dir_name+'/'
    create_pgs_directory(export_dir, 1)

    scores_list_file = new_ftp_dir+'/pgs_scores_list.txt'

    data = get_all_pgs_data(rest_url_root)

    print('\t- Fetch release dates')
    current_release = get_latest_release(rest_url_root)
    current_release_date = current_release['date']
    previous_release_date = get_previous_release(rest_url_root)['date']
 
    archive_file_name = '{}/../pgs_ftp_{}.tar.gz'.format(export_dir,current_release_date)

    score_ids_list = [ x['id'] for x in data['score'] ]

    # Generate file listing all the released Scores
    generate_scores_list_file(scores_list_file,score_ids_list)

    # Generate all PGS metadata export files
    call_generate_all_metadata_exports(export_dir,data,debug,current_release_date)

    # Generate PGS metadata export files for each released studies
    call_generate_studies_metadata_exports(export_dir,data,debug)

    # Build FTP structure for metadata files
    build_metadata_ftp(export_dir,new_ftp_dir,score_ids_list,previous_release_date,use_remote_ftp,debug)

    # Check that the new entries have a PGS directory
    check_new_data_entry_in_metadata(new_ftp_dir,data,current_release)

    # Build FTP structure for the bulk metadata files
    build_bulk_metadata_ftp(export_dir,new_ftp_dir,previous_release_date,use_remote_ftp)

    # Generates the compressed archive to be copied to the EBI Private FTP
    tardir(new_ftp_dir, archive_file_name)

if __name__ == '__main__':
    main()