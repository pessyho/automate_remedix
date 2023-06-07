#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""

    automate_remedix

    An sftp process to download the remedix input file, and upload the POD daily data in pdf format
    Also calls an artisan script to autmate the entire upload,validation and cvrp process of remedix

    0.9 beta initial
    1.0 initial version - db + config init, download from server to server
    1.1 run_mk_pdf  & upload_pods_to_remedix
    1.2 added cmdline parsing,  cvrp  and parsing of json response on artisan pod:save
    1.3 credentials and dirs in config

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
import socket
import pandas as pd
import argparse


_VERSION = '1.3'

# db connectors
db_connector = None
ssh_tunnel_host = None
server = None
dss_config = None
_python39 = False


port = None
host = None
user = None
password = None
download_to_server_dir = None
upload_pod_from_server_dir = None
artisan_dir = None


def cmdline():
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', action='version', version='%(prog)s {}'.format(_VERSION))
    parser.add_argument('--download', '-D', action='store_true', help='download remedix input file')
    parser.add_argument('--pdf', '-P', action='store_true', help='create pdf files')
    parser.add_argument('--upload', '-U', action='store_true', help='upload pdf ouput files')
    parser.add_argument('--cvrp', '-C', action='store_true', help='run cvrp')
    return parser.parse_args()


def get_running_server_ip():
    try:
        # Create a dummy socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(("google.com", 80))  # Connect to a remote server (e.g., google.com) with a known IP

        # Get the socket address
        ip_address = sock.getsockname()[0]

        # Close the dummy socket
        sock.close()

        return ip_address
    except socket.error as e:
        print(f"Error retrieving server IP: {e}")
        return None


def load_config(dss=False):

    config_file = None
    try:
        if not dss:
            wd = os.getcwd()
            config_file = os.path.join(wd, "config/automate_rx.json")
        else:
            wd = os.environ.get('DSS_WD', '.')
            config_file = os.path.join(wd, "config/dss-config.json")
        if not os.path.isfile(config_file):
            logging.error('ERR missing automate_rx.json configuraion file ')
            return False, None

        with open(config_file) as json_data_file:
            data = json.load(json_data_file)
    except Exception as e:
        logging.error(f'load_config(dss={dss}) Exception: {e} ')
        return False, None

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


def exec_subprocess(cmd):

    if _platform == 'darwin': # running from MAC
        # SSH command to execute
        ssh_command = "ssh pessyhollander@app.cibeez.dev.helmes.ee " + cmd
        # Execute the SSH command
        try:
            result = subprocess.check_output(ssh_command, shell=True)
            print(result.decode())
        except subprocess.CalledProcessError as e:
            print(f"Error executing SSH command: {e}")
            return False, None
    else: # from local machine
        try:
            result = subprocess.check_output(cmd, shell=True)
            print(result.decode())
        except subprocess.CalledProcessError as e:
            print(f"Error executing shell command: {e}")
            return False, None

    return True, result


"""
    run_cvrp(file_name) . Execute automated script  after download:

        input: file_name, the downloaded file name, str
        output: success, True/False

    1. go to directory /var/www/html/order-capture-implementation
    2. run this cmd  php artisan remedix:tocvrp /{foldername}/{filename} 
    {foldername} is 
    download_to_server_dir = '/var/www/html/order-capture-implementation/storage/remedixfiles/'

"""
def run_cvrp(downloaded_file):
    cvrp_cmd = '\'cd '+artisan_dir+';  php artisan remedix:tocvrp ' + download_to_server_dir + downloaded_file +"'"
    ok_exec, ret_exec = exec_subprocess(cvrp_cmd)
    return ok_exec, ret_exec


"""

PDF artisan

    1. cd /var/www/html/order-capture-implementation
    2. pod:save "10141784,10151082,5555555555,10186911,10189742,10184283,8924338"

    {
          "status": "OK",
          "stored": 4,
          "dir": "/var/www/html/order-capture-implementation/storage/pods/Remedix",
          "pods": [
            [
              "991831920",
              "/var/www/html/order-capture-implementation/storage/pods/Remedix/8M23018650_20230531.pdf"
            ],
            [
              " 991831921",
              "/var/www/html/order-capture-implementation/storage/pods/Remedix/8M23018652_20230531.pdf"
            ],
            [
              " 991840694",
              "/var/www/html/order-capture-implementation/storage/pods/Remedix/8U21035594_20230531.pdf"
            ],
            [
              " 991840695",
              "/var/www/html/order-capture-implementation/storage/pods/Remedix/8U21035723_20230531.pdf"
            ]
          ]
    }

"""
def run_mk_pdf(db_connector, o_req_uid):
    ret_exec = None
    ok_exec = False
    # first get all orders delivered today
    order_list = pd.DataFrame()
    this_sql = 'SELECT id, o_external_id, o_order_state, updated_at FROM orders WHERE o_order_state IN (8,15) \
    AND o_req_uid = {0} AND DATE(updated_at)="{1}";'.format(o_req_uid,dt.datetime.now().strftime("%Y-%m-%d"))
    try:
        order_list = pd.read_sql(this_sql, db_connector)
        if not order_list.empty:
            orders_ids = ('"' + str(order_list['id'].tolist()) + '"').replace('[', '').replace(']', '')
            logging.debug(f'pdf order list {orders_ids}')
            upload_cmd = 'cd '+artisan_dir+';  php artisan pod:save "' + orders_ids + '"'
            logging.debug(f'exceuting upload cmd:  {upload_cmd}')
            ok_exec, ret_exec = exec_subprocess(upload_cmd)
            logging.debug(f'exec_subprocess() {ok_exec} response:   {ret_exec}')
        else:
            logging.warning(f'read_sql returned empty, no pod found to upload to remedix. (sql: {this_sql})')
    except Exception as e:
        logging.error(f'run_mk_pdf() exception: {e}')


    return ok_exec, ret_exec


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
        msg = f'sfpt exception: {e}'
        logging.error(msg)
        print(msg)
        return None, None

    return sftp, transport


"""
    download_from_remedix
"""
def download_from_remedix(sftp):


    if sftp:

        remote_files = sftp.listdir('From remedix')  # list files in dir
        f_size = 0
        f_time = 0

        # file_pattern = 'CiBeez_NEXTDAY_240523*.txt' # debug
        file_pattern = 'CiBeez_NEXTDAY_{0}*.txt'.format(dt.datetime.now().strftime("%d%m%y"))
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
                download_cmd = 'sshpass -p '+ password + ' sftp '+ user+ '@'+host+':"/From\ Remedix/"'  + download_this +  ' '+ download_to_server_dir
                ok_exec, ret_exec = exec_subprocess(download_cmd)
                if not ok_exec:
                    return False, None
            else:
                local_dir = download_to_server_dir
                try:
                    sftp.get(download_this, '{0}{1}'.format(local_dir, download_this))
                    msg = "{0} downloaded to {1}".format(download_this, local_dir)
                    print(msg)
                    logging.debug(msg)
                except Exception as e:
                    msg = f'sftp.get() Exception: {e}'
                    print(msg)
                    logging.debug(msg)
                    return False, None

            return True, download_this
        else:
            msg = "Nothing to download..."
            print(msg)
            logging.debug(msg)
            return False, None

    return False, None

def upload_pod_to_remedix(sftp):
    if sftp:
        try:
            today = dt.datetime.now().strftime("%d%m%y")
            sftp.put(f'{upload_pod_from_server_dir}*_{today}', '/From\ Cibeez/.' )
            msg = f'Uploaded pods to remedix for today: {today}'
            print(msg)
            logging.debug(msg)
        except Exception as e:
            msg = f'sftp.get() Exception: {e}'
            print(msg)
            logging.debug(msg)
            return False
    return True

if __name__ == "__main__":

    args = cmdline()

    python_ver = str(sys.version_info[0]) + '.' + str(sys.version_info[1]) + '.' + str(sys.version_info[2])
    _python39 = int(python_ver.split('.')[1]) >= 9  # means >= '3.9.xxx'
    print('automate_remedix version: {0}, python version: {1}'.format(_VERSION, python_ver ))
    logging.basicConfig(filename='./log/automate_remedix.log',
                        format="%(asctime)s — %(name)s — %(levelname)s — %(funcName)s:%(lineno)d — %(message)s")
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    server_ip = get_running_server_ip()
    prod_server_ip = '10.200.4.214' == server_ip
    if server_ip:
        logging.info(f"The IP address of the running server is: {server_ip}, {'prod' if prod_server_ip else 'dev'} server")

    ok_config, configs = load_config()
    ok_config_dss, dss_config = load_config(True)
    if not ok_config or not ok_config_dss:
        print("cant load config file, terminating ...")
        exit()
    port = int(configs.get('auth').get('port'))
    host = configs.get('auth').get('host')
    user = configs.get('auth').get('user')
    password = configs.get('auth').get('password')
    download_to_server_dir = configs.get('dir').get('download_to_server_dir')
    upload_pod_from_server_dir = configs.get('dir').get('upload_pod_from_server_dir')
    artisan_dir = configs.get('dir').get('artisan_dir')


    db_connector, ssh_tunnel_host, server = init_db()
    today = dt.datetime.now().strftime("%d%m%y")
    remedix_input_file = None
    sftp, transport = connect_to_sftp(host, port, user, password, None, 'user_pass')
    if sftp:
        for cmd, val in vars(args).items():
            if cmd == 'download' and val:
                ok_download, remedix_input_file = download_from_remedix(sftp)
            elif cmd == 'pdf' and val:
                ok_mk_pdf, ret_exec = run_mk_pdf(db_connector, 87 if prod_server_ip else 83)
            elif cmd == 'upload' and val:
                ok_upload = upload_pod_to_remedix(sftp)
            elif cmd == 'cvrp' and val:
                ok_cvrp = run_cvrp(remedix_input_file)

        sftp.close()

    logging.debug("Done.")
    print("Done.")



