import time
import logging
import os
from lib.ftp_logger_to_database import FtpLogHookMelsecIQF
from lib.ftp_logger_to_database import DbLogHookPostgreSql

def main():
    logging.basicConfig(filename = os.getcwd() + '\\logs\\main.log', level=logging.INFO, format = '%(asctime)s:%(name)s:%(message)s')

    ## DATABASE constants
    HOST = 'localhost'
    DBNAME = 'plchistorian'
    USER = 'postgres'
    PASSWORD = 'password'
    SCHEMA_NAME = '0001'
    TABLE_NAME = '0001'

    ## PLC constants (dafaults for MELSEC iQ-F)
    PLC_LOG_PATH = 'LOGGING\\LOG01\\'
    PLC_ADDRESS = '192.168.3.250'
    PLC_FTP_LOGIN = 'FXCPU'
    PLC_FTP_PASSWORD = 'FXCPU'

    ## local directory for this PLC
    LOCAL_DIRECTORY = 'C:\\Users\\alexa\\FTP\\'

    start_time = time.time()

    try:
        logging.info("Begin import fom PLC to local directory")
        plc = FtpLogHookMelsecIQF(LOCAL_DIRECTORY, PLC_ADDRESS, PLC_LOG_PATH, PLC_FTP_LOGIN, PLC_FTP_PASSWORD)
        plc.import_log_files_from_plc_to_local_folder()
    except Exception as e:
        logging.error(e)

    try:
        logging.info("Begin import fom local directory to database")
        db = DbLogHookPostgreSql(LOCAL_DIRECTORY, HOST, DBNAME, USER, PASSWORD, SCHEMA_NAME, TABLE_NAME)
        db.import_log_files_from_local_folder_to_db()
    except Exception as e:
        logging.error(e)

    logging.info("Time spent                                    :   " + str(time.time()-start_time) + ' s')


if __name__ == '__main__':
    main()