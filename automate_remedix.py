#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""

    automate_remedix

    An sftp process to download the remedix input file, and upload the POD daily data in pdf format
    Also calls an artisan script to autmate the entire upload,validation and cvrp process of remedix

    0.9 beta initial
    1.0 initial version - db + config init, download from server to server

"""

"""
    Execute automated script after download:
    
    1. go to directory /var/www/html/order-capture-implementation
    2. run this cmd  php artisan remedix:tocvrp /{foldername}/{filename} 

"""

"""

PDF artisan

    1. cd /var/www/html/order-capture-implementation
    2. pod:save "10141784,10151082,5555555555,10186911,10189742,10184283,8924338"
    
markallanconrad@app:/var/www/html/order-capture-implementation$ php artisan pod:save "10141784,10151082,5555555555,10186911,10189742,10184283,8924338"
Array
(
    [status] => OK
    [stored] => 4
    [dir] => /var/www/html/order-capture-implementation/storage/pods/Remedix
    [pods] => Array
        (
            [0] => Array
                (
                    [0] => 10141784
                    [1] => /var/www/html/order-capture-implementation/storage/pods/Remedix/20230518_10141784.pdf
                )

            [1] => Array
                (
                    [0] => 10151082
                    [1] => /var/www/html/order-capture-implementation/storage/pods/Remedix/20230518_10151082.pdf
                )

            [2] => Array
                (
                    [0] => 5555555555
                    [1] => order not found
                )

            [3] => Array
                (
                    [0] => 10186911
                    [1] => /var/www/html/order-capture-implementation/storage/pods/Remedix/20230518_10186911.pdf
                )

            [4] => Array
                (
                    [0] => 10189742
                    [1] => /var/www/html/order-capture-implementation/storage/pods/Remedix/20230518_10189742.pdf
                )

            [5] => Array
                (
                    [0] => 10184283
                    [1] => order is not from Remedix
                )

            [6] => Array
                (
                    [0] => 8924338
                    [1] => pod not found
                )

        )

)
"""

import paramiko
import logging
import datetime as dt
import os
import sys
from sys import platform as _platform
import mysql.connector
from mysql.connector import errorcode
import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError
import json
import fnmatch
import subprocess


sftp_remedix_v = '1.0'

# db connectors
db_connector = None
ssh_tunnel_host = None
server = None
dss_config = None
_python39 = False


port = 22
host = 'remedix.packetauth.com'
user = 'Ucibeez'
password = 'Remedix@2023!'
download_to_server_dir = '/var/www/html/order-capture-implementation/storage/remedixfiles/'
download_pod_from_server_dir = '/var/www/html/order-capture-implementation/storage/pods/Remedix/'


def dss_load_config():
    wd = os.environ.get('DSS_WD', '.')
    dss_config_file = os.path.join(wd, "config/dss-config.json")
    if not os.path.isfile(dss_config_file):
        logging.error('ERR missing dss-config.json configuraion file ')
        return False, None

    with open(dss_config_file) as json_data_file:
        data = json.load(json_data_file)

    #global _dev
    #_dev = data.get('dss-locker-url').find('dev') != -1
    #mylog('dss-server running on {} env'.format("DEV" if _dev else "PROD"), 'debug')


    return True, data

def init_db():

    global _python39

    # db config , init db connector and sshtunnel
    wd = os.environ.get('DSS_WD', '.')
    option_files = os.path.join(wd, "config/dss-mysql.cnf")
    if not os.path.isfile(option_files):
        errmsg = 'ERR missing dss-mysql.cnf configuraion file '
        logging.error(errmsg)
        return None, None, None

    config = {
        'option_files': option_files,  # "'dss-mysql.cnf',
        'option_groups': ['Client', 'DSS']
    }

    # -------------------------------------------------------------
    # this peace of code is used only when testing from a client
    # -------------------------------------------------------------
    logging.debug("Executed from " + _platform + " machine ")
    # when using from client, set to your own config
    ssh_tunnel_host = None
    try:
        ssh_tunnel_host = os.environ['DSS_TUNNEL_HOST']
        ssh_private_key = '/Users/pessyhollander/.ssh/id_rsa'
        ssh_username = 'pessyhollander'
    except:
        pass

    server = None

    if ssh_tunnel_host:
        import sshtunnel
        logging.info("Initiating sshtunnel from a MAC OS machine ")
        print("Initiating sshtunnel from a MAC OS machine ")
        try:
            server = sshtunnel.SSHTunnelForwarder(
                (ssh_tunnel_host, 22),
                ssh_private_key=ssh_private_key,
                ssh_username=ssh_username,
                remote_bind_address=('127.0.0.1', 3306),
            )
            server.start()
            connected_port = server.local_bind_port
            logging.debug("connected to port: {0}".format(connected_port))
        except Exception as e:
            logging.error('sshtunnel forwarding failed: {0}'.format(e))
            return None, None, None

        config['host'] = '127.0.0.1'
        config['port'] = connected_port
    # ------------------------------------------------------------

    try:
        if not _python39: # we still use it in v.3.7.5 for compatability, will be removed later
            db_connector = mysql.connector.connect(**config)
            logging.debug("_python39 is False, using db_connector: {0}".format(db_connector))
        else:
            connection_data = 'mysql+pymysql://{user}:{password}@{host}:{port}/{db}'.format(user=dss_config.get('sqlalchemy').get('user'),
                                                                                            password=dss_config.get('sqlalchemy').get('password'),
                                                                                            host=dss_config.get('sqlalchemy').get('host'),
                                                                                            port=dss_config.get('sqlalchemy').get('port') if not ssh_tunnel_host else config['port'],
                                                                                            db=dss_config.get('sqlalchemy').get('db'))
            engine = sqlalchemy.create_engine(connection_data, echo=False)
            db_connector = engine.connect()
            logging.debug("_python39 is True, using sqlalchemy engine and connection : {0}".format(db_connector))

        logging.debug("Connected to db, db connection: {0}".format(db_connector))
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            logging.error("ERR: (mysql.connector) Something is wrong with your user name or password. err: ֻֻ{0}".format(
                err.errno))
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            logging.error("ERR: (mysql.connector) Database does not exist. err: ֻֻ{0}".format(err.errno))
        else:
            logging.error("ERR: (mysql.connector) {0}".format(err))
        return None, None, None
    except SQLAlchemyError as err:
        logging.error("ERR: SQLAlchemyError err (create_engine) {0}".format(str(err.__dict__['orig'])))
        return None, None, None

    return db_connector, ssh_tunnel_host, server

"""
    run_cvrp_automation(file_name) . Execute automated script  after download:
    
        input: file_name, the downloaded file name, str
        output: success, True/False
        
    1. go to directory /var/www/html/order-capture-implementation
    2. run this cmd  php artisan remedix:tocvrp /{foldername}/{filename} 
    {foldername} is 
    download_to_server_dir = '/var/www/html/order-capture-implementation/storage/remedixfiles/'
    
    
"""
def run_cvrp_automation(downloaded_file):
    print('soon...')


#Auth types: user_pass, key_only, key_and_pass
#You can pass a junk string in for password or sftp_key if not used
def connect_to_sftp(host, port, username, password, sftp_key, auth_type):
    try:
        transport = paramiko.Transport((host, port))
        if auth_type == "key_and_pass":
            sftp_key = paramiko.RSAKey.from_private_key_file(sftp_key)
            transport.start_client(event=None, timeout=15)
            transport.get_remote_server_key()
            transport.auth_publickey(username, sftp_key, event=None)
            transport.auth_password(username, password, event=None)
            #transport.connect(username = username, password = password, pkey = sftp_key)
        elif auth_type == "key_only":
            sftp_key = paramiko.RSAKey.from_private_key_file(sftp_key)
            transport.connect(username = username, pkey = sftp_key)
        elif auth_type == "user_pass":
            transport.connect(username = username, password = password)
        else:
            msg = "connect_to_sftp err, uknown auth_type {}".format(auth_type)
            logging.log(logging.WARNING, msg)
            print(msg)
        sftp = paramiko.SFTPClient.from_transport(transport)
    except Exception as e:
        msg = "sfpt exception: {}".format(e)
        logging.log(logging.WARNING, msg)
        print(msg)

    return sftp, transport


"""
    download_from_remedix
"""
def download_from_remedix(sftp):
    if sftp:

        remote_files = sftp.listdir('From remedix')  # list files in dir
        f_size = 0
        f_time = 0

        #file_pattern = 'CiBeez_NEXTDAY_{0}*.txt'.format(dt.datetime.now().strftime("%d%m%y"))
        file_pattern = 'CiBeez_NEXTDAY_240523*.txt' # debug
        selected_files = fnmatch.filter(remote_files, file_pattern)

        if selected_files != []:
            for file in selected_files:
                print('file: {}, size: {}, time: {}'.format(file, sftp.stat(file).st_size, sftp.stat(file).st_mtime))
                #lstatout = str(sftp.lstat(file)).split()[0]
                #if 'd' not in lstatout:
                #    print('file: {0}'.format(file))
                if sftp.stat(file).st_size > f_size and sftp.stat(file).st_mtime > f_time:
                    download_this = file
            msg = "found nd file to download : {}".format(download_this)
            print(msg)
            logging.debug(msg)
            # download file
            if _platform == 'darwin': # on my MAC
                local_dir = './download/'
            else:
                local_dir = download_to_server_dir
            sftp.get(download_this, '{0}/{1}'.format(local_dir, download_this))
            msg = "{0} downloaded to {1}".format(download_this, local_dir)
            print(msg)
            logging.debug(msg)
            return True, download_this
        else:
            msg = "Nothing to download..."
            print(msg)
            logging.debug(msg)
            return False, None
    return False, None

if __name__ == "__main__":

    python_ver = str(sys.version_info[0]) + '.' + str(sys.version_info[1]) + '.' + str(sys.version_info[2])
    _python39 = int(python_ver.split('.')[1]) >= 9  # means >= '3.9.xxx'
    print('sftp_remedix version: {0}, python version: {1}'.format(sftp_remedix_v,python_ver ))
    logging.basicConfig(filename='./log/sftp_remedix.log',
                        format="%(asctime)s — %(name)s — %(levelname)s — %(funcName)s:%(lineno)d — %(message)s")
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    ok_cnf, dss_config = dss_load_config()

    db_connector, ssh_tunnel_host, server = init_db()
    today = dt.datetime.now().strftime("%d%m%y")
    #download_file = 'CiBeez_140523'
    #download_file = 'CiBeez_' + today  # this is for testing
    download_file = 'CiBeez' + '_NEXTDAY_' + today
    sftp, transport = connect_to_sftp(host, port, user, password, None, 'user_pass')
    if sftp:
        ok_download, file_name = download_from_remedix(sftp)

    logging.debug("Done.")



