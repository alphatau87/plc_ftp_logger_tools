import io 
import os 
import psycopg2 
import logging
from abc import ABC, abstractmethod
from ftplib import FTP


class FtpLogHook(ABC):

    def __init__(self, local_folder, plc_host, plc_ftp_log_path, plc_ftp_username, plc_ftp_password):
        self.local_folder = local_folder
        self.plc_host = plc_host
        self.plc_ftp_log_path = plc_ftp_log_path
        self.plc_ftp_username = plc_ftp_username
        self.plc_ftp_password = plc_ftp_password
        
    @abstractmethod
    def import_log_files_from_plc_to_local_folder(self): pass


class DbLogHook(ABC):

    def __init__(self, local_folder, db_host, db_name, db_username, db_password, db_schema, db_table):
        self.local_folder = local_folder
        self.db_name = db_name
        self.db_host = db_host
        self.db_username = db_username
        self.db_password = db_password
        self.db_schema = db_schema
        self.db_table = db_table
        
    @property
    def db_table_full_name(self):
        return """\"""" + self.db_schema + """\"""" + "." + """\"""" + self.db_table + """\""""

    @property
    def db_connection_string(self):
        return f"host='{self.db_host}' dbname='{self.db_name}' user='{self.db_username}' password='{self.db_password}'"

    @abstractmethod
    def import_log_files_from_local_folder_to_db(self): pass


class FtpLogHookMelsecIQF(FtpLogHook):

    def import_log_files_from_plc_to_local_folder(self):
        files_found_on_server = 0
        files_imported_from_server = 0
        files_deleted_from_server = 0

        # clean up local_folder before importing
        for file_name in os.listdir(self.local_folder):
            file_url=os.path.join(self.local_folder, file_name)
            if (file_name.endswith(".csv.tmp") or file_name.endswith(".CSV.TMP")):
                os.remove(file_url)
                logging.warning("removed " + file_url)

        # timeout set to 3 second, but in very slow networks can be set higher
        with FTP(self.plc_host, timeout=3) as ftp:
            # MELSEC FX5U only supports Active mode
            ftp.passiveserver=0
            ftp.login(self.plc_ftp_username, self.plc_ftp_password)
            listing = ftp.nlst(self.plc_ftp_log_path)

            folder_listing = []
            for full_path in listing:
                if self.plc_ftp_log_path in full_path:
                    path = full_path.replace(self.plc_ftp_log_path,'')
                    if ((len(path) == 8) and (path.find('.') == -1)):
                        folder_listing.append(full_path)

            for folder in folder_listing:
                file_name_list_in_this_folder = ftp.nlst(folder)

                for file_name in file_name_list_in_this_folder:
                    if (file_name.endswith(".csv") or file_name.endswith(".CSV")):
                        try:
                            files_found_on_server += 1
                            file_name_local = self.local_folder + file_name.replace(folder,'')
                            with open(file_name_local + '.TMP', 'wb') as file:
                                ftp.retrbinary('RETR ' + file_name, file.write)
                                logging.debug(file_name + " downloaded to local folder from PLC")
                                files_imported_from_server += 1
                                ftp.delete(file_name)
                                logging.debug(file_name + " deleted from PLC")
                                files_deleted_from_server += 1
                        except Exception as e:
                            logging.warning(e)
                        else:
                            os.rename(file_name_local + '.TMP', file_name_local)
        
        logging.info("files found on server                         :   " + str(files_found_on_server))
        logging.info("files imported from server                    :   " + str(files_imported_from_server))
        logging.info("files deleted from server                     :   " + str(files_deleted_from_server)) 


class DbLogHookPostgreSql(DbLogHook):
    
    def import_log_files_from_local_folder_to_db(self):
        files_found_in_local_folder = len(os.listdir(self.local_folder))
        files_tried_to_import_to_db_from_local_folder = 0
        files_imported_to_db_from_local_folder = 0

        conn = psycopg2.connect(self.db_connection_string)

        for file_name in os.listdir(self.local_folder):

            file_url=os.path.join(self.local_folder, file_name)

            if (file_name.endswith(".csv") or file_name.endswith(".CSV")):
                files_tried_to_import_to_db_from_local_folder+=1
                try:
                    with open(file_url, 'r+') as file:
                        for x in range(3):
                            next(file)

                        with conn:
                            with conn.cursor() as cur:
                                cur.copy_from(file, self.db_table_full_name, sep=',')

                except Exception as e:
                    logging.warning(e)

                else:
                    try:
                        os.remove(file_url)

                    except Exception as e:
                        logging.warning(e)
                        logging.warning(file_url + " imported to DB, but not deleted from local folder")

                    else:
                        logging.debug(file_url + " imported to DB and deleted from local folder")
                    
                    files_imported_to_db_from_local_folder+=1
        
        conn.close()

        logging.info("files found in local folder                   :   " + str(files_found_in_local_folder))
        logging.info("files_tried_to_import_to_db_from_local_folder :   " + str(files_tried_to_import_to_db_from_local_folder))
        logging.info("files_imported_to_db_from_local_folder        :   " + str(files_imported_to_db_from_local_folder)) 


