#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""

    automate_remedix

    An sftp process to download the remedix input file, and upload the POD daily data in pdf format
    Also calls an artisan script to autmate the entire upload,validation and cvrp process of remedix

    0.9 beta initial

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
# need to import dsslib
import sys
#sys.path.insert(0, '/Users/pessyhollander/Documents/CiBeez/SW_Dev//omni-dss/omni-dss')
#import dsslib

sftp_remedix_v = '0.9'

port = 22
host = 'remedix.packetauth.com'
user = 'Ucibeez'
password = 'Remedix@2023!'
download_to_server_dir = '/var/www/html/order-capture-implementation/storage/remedixfiles/'
download_pod_from_server_dir = '/var/www/html/order-capture-implementation/storage/pods/Remedix/'

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





if __name__ == "__main__":
    print('sftp_remedix version: {0}'.format(sftp_remedix_v ))
    logging.basicConfig(filename='./log/sftp_remedix.log',
                        format="%(asctime)s — %(name)s — %(levelname)s — %(funcName)s:%(lineno)d — %(message)s")
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    today = dt.datetime.now().strftime("%d%m%y")
    #download_file = 'CiBeez_140523'
    #download_file = 'CiBeez_' + today  # this is for testing
    download_file = 'CiBeez' + '_NEXTDAY_' + today
    sftp, transport = connect_to_sftp(host, port, user, password, None, 'user_pass')
    if sftp:
        files = sftp.listdir('From remedix')  # list files in dir
        # find match in files
        file_nd_found = [match for match in files if download_file in match]
        f_size = 0
        f_time = 0
        if file_nd_found != []:
            for file in file_nd_found:
                print('file: {}, size: {}, time: {}'.format(file, sftp.stat(file).st_size, sftp.stat(file).st_mtime))
                #lstatout = str(sftp.lstat(file)).split()[0]
                #if 'd' not in lstatout:
                #    print('file: {0}'.format(file))
                if sftp.stat(file).st_size > f_size and sftp.stat(file).st_mtime > f_time:
                    download_this = file
            msg = "found nd file to download : {}".format(download_this)
            print(msg)
            logging.debug(msg)
            #logger.warning(msg)
            # download file
            sftp.get(download_this, './download/{0}'.format(download_this))
            msg = "{} downloaded to ./download/.".format(download_this)
            print(msg)
            logging.debug(msg)
        else:
            msg = "Nothing to download..."
            print(msg)
            logging.debug(msg)


