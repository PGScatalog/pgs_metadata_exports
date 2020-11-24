import sys, os
import hashlib
import requests
import urllib
from ftplib import FTP
from pgs_exports.PGSBuildFtp import PGSBuildFtp


class PGSBuildFtpRemote(PGSBuildFtp):

    ftp_root = 'ftp.ebi.ac.uk'
    ftp_path = 'pub/databases/spot/pgs/'


    def get_ftp_file(self,ftp_filename,new_filename):
        """ Download data file from the PGS FTP. """

        path = self.ftp_path
        # Metadata file
        if self.type == 'metadata':
            if self.pgs_id == 'all':
                path += self.meta_dir.lower()
                #filename = self.all_meta_file
            else:
                path += self.data_dir+self.pgs_id+self.meta_dir
                #filename = self.pgs_id+self.file_suffix
        # Score file
        else:
            path += self.data_dir+self.pgs_id+'/'+self.scoring_dir
            #filename = self.pgs_id+self.file_suffix

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
        else:
            filepath += self.scoring_dir+self.pgs_id+self.file_suffix

        try:
            ftp.retrbinary('RETR %s' % filepath, m.update)
            return m.hexdigest()
        except:
            print("Can't find or access FTP file: "+self.ftp_root+'/'+filepath)
