import sys, os
import hashlib
import shutil
from ftplib import FTP


#-------------------#
# Class PGSBuildFtp #
#-------------------#

class PGSBuildFtp:
    """ Fetch files from FTP and compare them with the newly generated files. """

    ftp_path = '/nfs/ftp/public/databases/spot/pgs/'
    allowed_types = ['score','metadata','publication']
    all_meta_file = 'pgs_all_metadata.tar.gz'
    data_dir = '/scores/'
    scoring_dir = '/ScoringFiles/'
    meta_dir    = '/Metadata/'
    pub_dir     = meta_dir.lower()+'publications/'
    meta_file_extension = '.tar.gz'


    def __init__(self, pgs_id, file_suffix ,type):
        self.pgs_id = pgs_id
        self.file_suffix = file_suffix
        if type in self.allowed_types:
            self.type = type
        else:
            print("The type '"+type+"' is not recognised!")
            exit()


    def get_ftp_file(self,ftp_filename,new_filename):
        """ Download data file from the PGS FTP. """

        path = self.ftp_path
        # Metadata file
        if self.type == 'metadata':
            if self.pgs_id == 'all':
                path += self.meta_dir.lower()
            else:
                path += self.data_dir+self.pgs_id+self.meta_dir
        # Publication metadata file
        elif self.type == 'publication':
            path += self.pub_dir+self.pgs_id+'/'
        # Score file
        else:
            path += self.data_dir+self.pgs_id+'/'+self.scoring_dir
        try:
            shutil.copy2(path+'/'+ftp_filename, new_filename)
        except IOError as e:
            print(f'Can\'t copy the FTP file {path}/{ftp_filename} to {new_filename}:\n{e}')
            

    def get_ftp_md5_checksum(self):
        """ Get the MD5 of the Excel spreadsheet on FTP to compare with current Excel spreadsheet. """

        filepath = self.ftp_path+self.data_dir+self.pgs_id+'/'
        if self.type == 'metadata':
            filepath += self.meta_dir+self.pgs_id+self.file_suffix
        elif self.type == 'publication':
            filepath = self.ftp_path+self.pub_dir+self.pgs_id+'/'+self.pgs_id+self.file_suffix
        else:
            filepath += self.scoring_dir+self.pgs_id+self.file_suffix

        try:
            md5 = self.get_md5_checksum(filepath)
            return md5
        except:
            print("Can't find or access FTP file: "+filepath)


    def get_md5_checksum(self,filename,blocksize=4096):
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




#-------------------------#
# Class PGSBuildFtpRemote #
#-------------------------#

class PGSBuildFtpRemote(PGSBuildFtp):
    """ Fetch files from FTP via FTP protocol and compare them with the newly generated files. """

    ftp_root = 'ftp.ebi.ac.uk'
    ftp_path = 'pub/databases/spot/pgs/'


    def get_ftp_file(self,ftp_filename,new_filename):
        """ Download data file from the PGS FTP. """

        path = self.ftp_path
        # Metadata file
        if self.type == 'metadata':
            if self.pgs_id == 'all':
                path += self.meta_dir.lower()
            else:
                path += self.data_dir+self.pgs_id+self.meta_dir
        # Publication metadata file
        elif self.type == 'publication':
            path += self.pub_dir+self.pgs_id+'/'
        # Score file
        else:
            path += self.data_dir+self.pgs_id+'/'+self.scoring_dir

        ftp = FTP(self.ftp_root)     # connect to host, default port
        ftp.login()                  # user anonymous, passwd anonymous@
        ftp.cwd(path)
        ftp.retrbinary("RETR " + ftp_filename, open(new_filename, 'wb').write)
        ftp.quit()


    def get_ftp_md5_checksum(self):
        """ Get the MD5 of the Excel spreadsheet on FTP to compare with current Excel spreadsheet. """

        ftp = FTP(self.ftp_root)     # connect to host, default port
        ftp.login()                  # user anonymous, passwd anonymous@

        m = hashlib.md5()
        filepath = self.ftp_path+self.data_dir+self.pgs_id+'/'
        if self.type == 'metadata':
            filepath += self.meta_dir+self.pgs_id+self.file_suffix
        elif self.type == 'publication':
            filepath = self.ftp_path+self.pub_dir+self.pgs_id+'/'+self.pgs_id+self.file_suffix
        else:
            filepath += self.scoring_dir+self.pgs_id+self.file_suffix

        try:
            ftp.retrbinary('RETR %s' % filepath, m.update)
            return m.hexdigest()
        except:
            print("Can't find or access FTP file: "+self.ftp_root+'/'+filepath)