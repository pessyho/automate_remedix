#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""

    automate_remedix

    An sftp process to download the remedix input file, and upload the POD daily data in pdf format
    Also calls an artisan script to autmate the entire upload,validation and cvrp process of remedix

    0.9     beta initial
    1.0     initial version - db + config init, download from server to server
    1.1     run_mk_pdf  & upload_pods_to_remedix
    1.2     added cmdline parsing,  cvrp  and parsing of json response on artisan pod:save
    1.3     credentials and dirs in config
    1.3.1   optional date parameter to cmds
    1.3.2   added run_cvrp report by email
    1.3.3   added download file validation by size comparison + retry if issue with the size mismatch
    1.3.4   add sms notification in case there is an issue with the sftp
    1.4     added new artisan tocvrp options/flags to have stepwise validation process
    1.4.1   sys.exit(0)
    1.4.2   connectto_tsp() -> Use paramiko.SSHClient() for _python39 not paramiko.Transport()  (Disable key-based auth by not passing pkey)
    1.4.2.1 fixes

"""

_VERSION = '1.4.2.1'

import os
import paramiko
import logging
import datetime as dt
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
import requests
import traceback


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
cvrp_dir = None
home_dir = None
tocvrp_options = ['V', 'S', 'C', 'VS', 'SC', 'VSC']

def cmdline():
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', action='version', version='%(prog)s {}'.format(_VERSION))
    #parser.add_argument('--date', '-C', action='store_true', help='run cvrp')
    #parser.add_argument('--download', '-D', action='store_true', help='download remedix input file')
    parser.add_argument('--download', '-D', nargs='?', const=True, type=str, help='download remedix input file')
    parser.add_argument('--pdf', '-P', nargs='?', const=True, type=str, help='create pdf files')
    parser.add_argument('--upload', '-U', nargs='?', const=True, type=str, help='upload pdf ouput files')
    parser.add_argument('--cvrp', '-C', nargs='*', action='append', type=str, help='run cvrp') # 0 or more args
    parser.add_argument('--deb', '-E', nargs='?', const=True, type=bool, help='deb')
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
            #config_file = os.path.join(wd, "config/automate_rx.json")
            logging.debug(f"wd: {wd}.")
            config_file = "./config/automate_rx.json"  # use relative directory because of crontab
            logging.debug(f'config_file {config_file}')
        else:
            wd = os.environ.get('DSS_WD', '.')
            logging.debug(f"DSS_WD: {wd}")
            config_file = os.path.join(wd, "config/dss-config.json")
            logging.debug(f'dss config_file {config_file}')
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
        ssh_command = f'ssh pessyhollander@app.cibeez.dev.helmes.ee {cmd}'
        # Execute the SSH command
        try:
            result = subprocess.check_output(ssh_command, shell=True)
            print(result.decode())
            logging.debug(f'exec_subprocess(cmd): {cmd} success, result: {result.decode("utf-8")}')
        except subprocess.CalledProcessError as e:
            errmsg = f'exec_subprocess(cmd)(). Error executing SSH command. code: {e.returncode}, output: {e.output.decode("utf-8")}'
            print(errmsg)
            logging.error(errmsg)
            return False, None
    else: # from local machine
        try:
            result = subprocess.check_output(cmd, shell=True)
            print(result.decode())
            logging.debug(f'exec_subprocess(cmd): {cmd} success, result: {result.decode("utf-8")}')
        except subprocess.CalledProcessError as e:
            errmsg = f'exec_subprocess(cmd)(). Error executing cmd {cmd}. code: {e.returncode}, output: {e.output.decode("utf-8")}'
            print(errmsg)
            logging.error(errmsg)
            return False, None

    return True, result


"""
    run_cvrp(file_name) . Execute automated script  after download:

        input: file_name, the downloaded file name, str
        output: success, True/False

    1. go to directory /var/www/html/order-capture-implementation
    2. run this cmd  php artisan remedix:tocvrp /{foldername}/{filename} 
    {foldername} is 
    cvrp_dir = '/remedixfiles/'
    
    artisan options
    php artisan remedix:tocvrp <path>  = do everything
    php artisan remedix:tocvrp <path> V = only do Validate
    php artisan remedix:tocvrp <path> S = only do Submit
    php artisan remedix:tocvrp <path> C = only do CVRP
    php artisan remedix:tocvrp <path> VS = do Validate & Submit
    php artisan remedix:tocvrp <path> SC = do Submit & CVRP
    php artisan remedix:tocvrp <path> VSC = do everything

"""


def run_cvrp(downloaded_file, option=''):

    ok_exec = False
    ret_exec = None

    if downloaded_file is None:
        report = f'run_cvrp() ERR, missing input file: {downloaded_file}. terminating..'
        logging.debug(report)
    else:
        cvrp_cmd = f'cd {artisan_dir}; php artisan remedix:tocvrp {cvrp_dir}{downloaded_file} {option}'
        if _platform == 'darwin': # in darwin/MAC its packed in an SSH so we need to pack the cmd in ""
            cvrp_cmd = f'"{cvrp_cmd}"'
        logging.debug(f'run_cvrp() cmd: {cvrp_cmd}')
        ok_exec, ret_exec = exec_subprocess(cvrp_cmd)
        logging.debug(f'run_cvrp(): {ok_exec}, raw result:  {ret_exec}')
        if ok_exec and ok_exec is not None:
            ret_exec_dict = json.loads(ret_exec.decode('utf-8'))
            #print(f"ret_exec_dict: {ret_exec_dict}")
            logging.debug(f"ret_exec_dict: {ret_exec_dict}")
            counts = ''
            for key, val in (ret_exec_dict.get("counts") or {}).items():
                counts = f'{counts}\n{key}: {val}'
            report = f'*** run_cvrp() report *** \n\n \
count summary:\n\t{counts}\n\n \
not ready for validation:\n\t{ret_exec_dict.get("not ready for validation")}\n\n \
not validated orders (כתובות):\n\t{ret_exec_dict.get("not validated orders")}\n\n \
\n*** end run_cvrp() report ***\n'
            logging.debug(f'{report} is being sent by email...')
        else:
            cvrp_cmd = cvrp_cmd.replace('"', '') # remove #
            report = f'run_cvrp() cmd failed.\n{cvrp_cmd}\n'
            logging.debug(report)

    if _platform == 'darwin':
        email_cmd = f'\'{home_dir}/email_cvrp_report.sh "{report}"\''
    else:
        email_cmd = f'{home_dir}/email_cvrp_report.sh "{report}"'
    ok_exec, ret_exec = exec_subprocess(email_cmd)
    return ok_exec, ret_exec


"""

    run_mk_pdf(db_connector, o_req_uid, this_date)
    
    PDF artisan

    1. cd /var/www/html/order-capture-implementation
    2. pod:save "10141784,10151082,5555555555,10186911,10189742,10184283,8924338"
    
"""
def run_mk_pdf(db_connector, o_req_uid, this_date):
    ret_exec = None
    ok_exec = False
    # first get all orders delivered today
    order_list = pd.DataFrame()
    if not this_date:
        this_date = dt.datetime.now().strftime("%Y-%m-%d")
    else:
        #format from %d%m%y to "%Y-%m-%d"
        this_date = dt.datetime.strptime(this_date, '%d%m%y').strftime('%Y-%m-%d')

    this_sql = 'SELECT id, o_external_id, o_order_state, updated_at FROM orders WHERE o_order_state IN (8,15) \
    AND o_req_uid = {0} AND DATE(updated_at)="{1}";'.format(o_req_uid, this_date)
    try:
        order_list = pd.read_sql(this_sql, db_connector)
        if not order_list.empty:
            orders_ids = ('"' + str(order_list['id'].tolist()) + '"').replace('[', '').replace(']', '')
            logging.debug(f'pdf order list {orders_ids}')
            upload_cmd = 'cd '+artisan_dir+';  php artisan pod:save ' + orders_ids
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
        if not _python39:
            # Use paramiko.Transport for Python 3.7.5
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
                transport.connect(None, username = username, password = password) # Disable key-based auth by not passing pkey
            else:
                msg = "connect_to_sftp err, unknown auth_type {}".format(auth_type)
                logging.log(logging.WARNING, msg)
                print(msg)
                send_sms_alert(msg)
            sftp = paramiko.SFTPClient.from_transport(transport)
            return sftp, transport

        else: # _python39 is True
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            if auth_type == "key_and_pass":
                key = paramiko.RSAKey.from_private_key_file(sftp_key)
                ssh.connect(
                    hostname=host,
                    port=port,
                    username=username,
                    password=password,
                    pkey=key,
                    look_for_keys=False,
                    allow_agent=False
                )
            elif auth_type == "key_only":
                key = paramiko.RSAKey.from_private_key_file(sftp_key)
                ssh.connect(
                    hostname=host,
                    port=port,
                    username=username,
                    pkey=key,
                    look_for_keys=False,
                    allow_agent=False
                )
            elif auth_type == "user_pass":
                ssh.connect(
                    hostname=host,
                    port=port,
                    username=username,
                    password=password,
                    look_for_keys=False,
                    allow_agent=False
                )
            else:
                msg = f"connect_to_sftp error: unknown auth_type {auth_type}"
                logging.warning(msg)
                print(msg)
                send_sms_alert(msg)
                return None, None

            sftp = ssh.open_sftp()
            return sftp, ssh

    except Exception as e:
        msg = f'sfpt exception: {e}'
        logging.error(msg)
        print(msg)
        #send_sms_alert(msg)
        return None, None




"""
    download_from_remedix
"""
def download_from_remedix(sftp,this_date=None):

    retry = 2

    while sftp and retry:


        if not this_date:
            this_date = dt.datetime.now().strftime("%d%m%y")
        #file_pattern = 'CiBeez_NEXTDAY_240523*.txt' # debug
        file_pattern = f'CiBeez_NEXTDAY_{this_date}*.txt'
        remote_files = sftp.listdir('From remedix')  # list files in dir
        selected_files = fnmatch.filter(remote_files, file_pattern)

        f_size = 0
        f_time = 0

        if selected_files != []:
            for file in selected_files:
                print('file: {}, size: {}, time: {}'.format(file, sftp.stat(file).st_size, sftp.stat(file).st_mtime))
                #lstatout = str(sftp.lstat(file)).split()[0]
                #if 'd' not in lstatout:
                #    print('file: {0}'.format(file))
                if sftp.stat(file).st_size > f_size and sftp.stat(file).st_mtime > f_time:
                    download_this = file
                    f_size = sftp.stat(file).st_size # store file size
                    f_time = sftp.stat(file).st_mtime # store file timestamp
                    timestamp = dt.datetime.strftime(dt.datetime.fromtimestamp(sftp.stat(file).st_mtime),"%Y-%m-%d %H:%S:%M")
            msg = f"found ND file to download: {download_this}, size: {f_size}, timestamp: {timestamp}"
            print(msg)
            logging.debug(msg)
            # download file
            if _platform == 'darwin': # on my MAC
                download_cmd = f'sshpass -p {password} sftp {user}@{host}:"/From\ Remedix/{download_this}" {download_to_server_dir}'
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

            # check that dopwnlaoded file size and org are the same ..
            downloaded_fsize = os.path.getsize(f"{local_dir}/{download_this}")
            if downloaded_fsize != f_size:
                msg = f"ERR Download validation: local file size {downloaded_fsize} DONT MATCH file size on shared drive: {f_size}. Retry: {retry}"
                print(msg)
                logging.debug(msg)
                #return False, None
                retry = retry - 1
            else:
                msg = f"SUCCESS Download validation: local file size: {downloaded_fsize} match file size on shared drive: {f_size}."
                print(msg)
                logging.debug(msg)
                return True, download_this
        else:
            msg = "Nothing to download..."
            print(msg)
            logging.debug(msg)
            return False, None

    return False, None

def upload_pod_to_remedix(sftp, this_date):
    count = 0
    if not this_date:
        this_date = dt.datetime.now().strftime("%Y%m%d") # use default "today"
        #format from %d%m%y to "%Y%m%d"
    else:
        this_date = dt.datetime.strptime(this_date, '%d%m%y').strftime('%Y%m%d')

    if sftp:
        try:
            #today = dt.datetime.now().strftime("%Y%m%d")
            with os.scandir(upload_pod_from_server_dir) as entries:
                for entry in entries:
                    if (f'{this_date}.pdf') in entry.name:
                        local_path_file = f'{upload_pod_from_server_dir}{entry.name}'
                        remote_file = f'/From Cibeez/{entry.name}'
                        msg = f'Upload pod file: {local_path_file} to remedix: {remote_file}'
                        print(msg)
                        logging.debug(msg)
                        sftp.put(local_path_file, remote_file, confirm=False )
                        msg = f'Uploaded pod file {entry.name} to remedix for today: {this_date}'
                        print(msg)
                        logging.debug(msg)
                        count += 1
        except Exception as e:
            msg = f'sftp.put() Exception: {e}'
            print(msg)
            logging.debug(msg)
            send_sms_alert(msg)
            return False
    msg = f'Uploaded total of {count} pod files to remedix for today: {this_date}'
    print(msg)
    logging.debug(msg)
    return True


sms019_config = {
"usr_201": {"usr": "cibeez", "pwd": "Jf6pob1A", "token": "eyJ0eXAiOiJqd3QiLCJhbGciOiJIUzI1NiJ9.eyJmaXJzdF9rZXkiOiIxODQ4NyIsInNlY29uZF9rZXkiOiIxNTQzMTQ5IiwiaXNzdWVkQXQiOiIwOS0wMS0yMDI0IDIyOjMyOjMxIiwidHRsIjo2MzA3MjAwMH0.lSe8jDYgwwgPuJ2s1-_ikMRCeHTkxdneZ2Q5OyLVFqo"},
"usr_268": {"usr": "cibeez2", "pwd": "Tx26u6Vy", "token" :"eyJ0eXAiOiJqd3QiLCJhbGciOiJIUzI1NiJ9.eyJmaXJzdF9rZXkiOiIxODQ4OSIsInNlY29uZF9rZXkiOiIxNTQzMTU2IiwiaXNzdWVkQXQiOiIwOS0wMS0yMDI0IDIyOjM0OjA2IiwidHRsIjo2MzA3MjAwMH0.HDM35rqhCal1IW4_vhIKWgdgrUMuIOWThf4J7IWOIME"},
"usr_335": {"usr": "cibeez3", "pwd": "Qo2pH5Uv", "token": "eyJ0eXAiOiJqd3QiLCJhbGciOiJIUzI1NiJ9.eyJmaXJzdF9rZXkiOiIxODQ5MSIsInNlY29uZF9rZXkiOiIxNTQzMTYyIiwiaXNzdWVkQXQiOiIwOS0wMS0yMDI0IDIyOjM0OjUzIiwidHRsIjo2MzA3MjAwMH0.09MEEHLER5rn1RjQcJL03lfFlU7UytgA6kEXhpbeKJU"},
}

pessy_phone = '053-285-2755'
hannan_phone = '054-888-4315'
alert_phone_numbers = [pessy_phone, hannan_phone]
def send_sms_alert(sms_text,source="Remedix"):

    usr_201 = sms019_config.get('usr_201').get('usr')
    usr_268 = sms019_config.get('usr_268').get('usr')
    usr_335 = sms019_config.get('usr_335').get('usr')
    pwd_201 = sms019_config.get('usr_201').get('pwd')
    pwd_268 = sms019_config.get('usr_268').get('pwd')
    pwd_335 = sms019_config.get('usr_335').get('pwd')
    token_201 = sms019_config.get('usr_201').get('token')
    token_268 = sms019_config.get('usr_268').get('token')
    token_335 = sms019_config.get('usr_335').get('token')


    if len(sms_text) <= 201:
        usr = usr_201
        pwd = pwd_201
        token = token_201
    elif len(sms_text) <= 268:
        usr = usr_268
        pwd = pwd_268
        token = token_268
    else:
        usr = usr_335
        pwd = pwd_335
        token = token_335

    for xphone in alert_phone_numbers:

        cln_phone_number = xphone.replace('-','') # remove hyphen from tel number
        # sms019_test_url = 'https://www.019sms.co.il:8090/api/test'
        sms019_url = 'https://www.019sms.co.il/api'
        #sms019_url = 'www.019sms.co.il/api'
        # "<user>\n" <username>" + usr + "</username>\n" "<password>" + pwd + "</password>\n" "</user>\n"
        sms019_fmt = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" \
                     "<sms>\n" \
                     "<user>\n <username>" + usr + "</username>\n</user>\n" \
                     "<source>" + source + "</source>\n" \
                                           "<destinations>\n"\
                                           "<phone id=\"{0}\">{1}</phone>\n" \
                                           "</destinations>\n" \
                                           "<message>{2}</message>\n" \
                                           "</sms>".format(xphone, cln_phone_number, sms_text)

        headers = {'content-type': 'application/xml', 'Authorization': 'Bearer {0}'.format(token), 'Accept-Charset': 'UTF-8'}
        logging.debug(f'sending sms019_fmt: {sms019_fmt}')

        retry = 1 # no retries meanwhile .... makes problems and delays ...
        while retry > 0:
            try:
                r = requests.post(url=sms019_url,data=sms019_fmt.encode('utf-8'), headers=headers,timeout=3)
                content = r.text
                if r.status_code != 200:
                    return False, 'send_sms err, Content is Unavailable'
                else:
                    result = xmltodict.parse(content)
                    status = int(result['sms']['status'])
                    if status != 0:
                        errmsg = 'ERR: from provider, status: {0} msg: {1} '.format(
                            result['sms']['status'], result['sms']['message'])
                        logging.debug(errmsg)
                        return False, f'send_sms err {errmsg}'
                    else:
                        okmsg = f'sent sms to {phone_number} ok. response is: {content}'
                        logging.debug(okmsg)
                        retry = 0
                        break
            #except urllib2.HTTPError as e:
            except requests.exceptions.RequestException as e:
                retry -= 1
                if not retry:
                    return False, f'send_sms err, RequestException: {str(e.code)}'
                time.sleep(0.5)
            except Exception as e:
                retry -= 1
                if not retry:
                    return False, f'send_sms err, generic exception: {e}, traceback {traceback.format_exc()}'
                time.sleep(0.5)

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
    cvrp_dir = configs.get('dir').get('cvrp_dir')
    home_dir = configs.get('dir').get('home_dir')

    db_connector, ssh_tunnel_host, server = init_db()
    #today = dt.datetime.now().strftime("%d%m%y")
    remedix_input_file = ''
    sftp, transport = connect_to_sftp(host, port, user, password, None, 'user_pass')
    if sftp:
        for cmd, val in vars(args).items():
            # NOTE: format for all date is "%d%m%y"
            this_option = ''
            this_input = val if isinstance(val, str) else None
            #if this_input and this_input.upper() in tocvrp_options:
            #    this_option = this_input.upper()
            #    this_input = None
            if cmd == 'download' and val:
                ok_download, remedix_input_file = download_from_remedix(sftp, this_input)
            elif cmd == 'pdf' and val:
                ok_mk_pdf, ret_exec = run_mk_pdf(db_connector, 87 if prod_server_ip else 83, this_input)
            elif cmd == 'upload' and val:
                ok_upload = upload_pod_to_remedix(sftp, this_input)
            elif cmd == 'cvrp' and val:
                if (len(val[0]) == 0): # no args, run cvrp with default options
                    msg = 'No arguments provided for cvrp, No task  executed.'
                    logging.debug(msg)
                    print(msg)
                else:
                    if (len(val[0]) == 2): # file name and tocvrp arg for artisan defined in cvrp_options
                        remedix_input_file = val[0][0]
                        if val[0][1] in tocvrp_options:
                            this_option = str(val[0][1])
                        else:
                            msg = f'Unknow argument list: {val[0]}'
                            logging.deb(msg)
                            print(msg)
                            break
                    elif val[0][0] in tocvrp_options: # check if cvrp artisan option
                        this_option = str(val[0][0])
                    else:  # remedix file name
                        remedix_input_file = str(val[0][0])
                    ok_cvrp, ret_exec = run_cvrp(remedix_input_file, this_option)
            elif cmd == 'deb' and val:
                msg = f"deb mode: {val}"
                logging.debug(msg)
                print(msg)

        sftp.close()

    logging.debug("Done.")
    print("Done.")
    sys.exit(0)




