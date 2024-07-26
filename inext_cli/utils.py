import socket
import logging
import sys
import os
import pandas as pd
from inext_cli.constants import FILE_EXTENSIONS

def is_url_reachable(url, port=443, timeout=3):
    try:
        socket.setdefaulttimeout(timeout)
        socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect((url, port))
        return True
    except socket.error as ex:
        print(ex)
        return False


def validate_connection(conf):
    return True



def guess_profile_format(f):
    '''
    Helper function to determine what file type a file is
    :param f: string path to file
    :return: string of the format
    '''
    ext = FILE_EXTENSIONS
    ftype = ''

    for format in ext:
        for e in ext[format]:
            if f.endswith(e):
                ftype = format
                break
        if ftype != '':
            break

    return ftype

def file_valid(f):
    if os.path.exists(f) and os.path.getsize(f) > 0:
        return True
    return False

def get_line_count(f):
    return int(os.popen(f'wc -l {f}').read().split()[0])

def concatonate_files(files):
    dfs = []
    for f in files:
        df = pd.read_csv(f,sep="\t",header=0)
        if len(df) > 0:
            dfs.append(df)
    if len(dfs)> 0:
        return pd.concat(dfs)
    else:
        return pd.DataFrame()
