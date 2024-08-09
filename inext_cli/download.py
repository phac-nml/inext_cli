from inext_cli.classes.download import download, download_params
import json
import os
import sys
import logging
from inext_cli.version import __version__
from argparse import (ArgumentParser, ArgumentDefaultsHelpFormatter, RawDescriptionHelpFormatter)
from inext_cli.constants import file_type_regex
from dataclasses import asdict
from inext_cli.classes.config import config
from inext_cli.classes.connect import conf_connect

def add_args(parser=None):
    if parser is None:
        parser = ArgumentParser(
            description="IRIDA Next Command line interface: Download",)
    parser.add_argument('-i','--input', type=str, required=True,help='Download sheet (tsv supported)')
    parser.add_argument('-c', '--config', type=str, required=False, help='Configuration file (json)')
    parser.add_argument('-o', '--outdir', type=str, required=True, help='Output directory to put results')
    parser.add_argument('-u', '--url', type=str, required=False, help='graphql api url')
    parser.add_argument('-t', '--token', type=str, required=False, help='API token')
    parser.add_argument("--id_col", type=str, required=False, help="Column containing IRIDA Next persistent project ID or sample name")
    parser.add_argument("--sub_folder_col", type=str, required=False, help="Column with value for subfolder")
    parser.add_argument("--fname_col", type=str, required=False, help="Column containing filename")
    parser.add_argument("--url_col", type=str, required=False, help="Column containing IRIDA Next provided url to attachment")
    parser.add_argument("--gid_col", type=str, required=False, help="Column containing IRIDA Next attachment id as fail over to url")
    parser.add_argument("--file_type", type=str, required=False, help="Retrieve this type of file (all, fasta, fastq, json, text, csv, tsv, genbank)",default='all')
    parser.add_argument("--workers", type=int, required=False, help="Number of workers to split the task into",default=1)
    parser.add_argument("--size", type=int, required=False, help="Max number of samples folders for root",default=5000)
    parser.add_argument("--delim", type=str, required=False, help="Folder delimeter",default='/')
    parser.add_argument('-V', '--version', action='version', version="%(prog)s " + __version__)
    return parser



def run_download(params):
    pdict = asdict(params)
    for p in pdict:
        if pdict[p] is None or pdict[p] == '':
            if p not in ['gid_col', 'url', 'token']:
                print(f'Error {p} parameter is not set correctly')
                sys.exit()
    download(params)



def run(cmd_args=None):
    if cmd_args is None:
        parser = add_args()
        cmd_args = parser.parse_args()
    analysis_parameters = vars(cmd_args)
    run_config_file = cmd_args.config
    input_config = {}
    if run_config_file is not None:
        with open(run_config_file) as fh:
            input_config = json.loads(fh.read())
    
    #ignore parameters in config file
    for p in analysis_parameters:
        if p not in input_config:
            input_config[p] = analysis_parameters[p]
    baseDir = input_config['outdir']
    manifestFile = os.path.join(baseDir,"manifest.txt")
    logFile = os.path.join(baseDir,"run.log")
    errorFile = os.path.join(baseDir,"err.txt")

    #convert file type name to regex string
    file_type = input_config['file_type']
    file_types = asdict(file_type_regex())
    download_regex = "."
    if file_type in file_types:
        download_regex = file_types[file_type]

    gql_config = config(input_path=None, 
                        id_col= None,
                        id_type=None,
                        access = conf_connect(url=input_config['url'],token=input_config['token']),
                        outputs=None,
                        skip_rows=False,
                        n_records = 0,
                        n_threads = 0,
                        n_cursors= 1,
                        skip_meta=True,
                        create=False,   
                        download= False,
                        download_regex = '.'                                        
                        )

    params = download_params(
            file_path = input_config['input'],
            sample_col = input_config['id_col'],
            filename_col = input_config['fname_col'],
            sub_folder_col = input_config['sub_folder_col'],
            url_col = input_config['url_col'],
            baseDir = baseDir,
            manifestFile = manifestFile,
            logFile = logFile,
            errorFile = errorFile,
            file_types = download_regex,
            gql_config = gql_config,
            n_workers = input_config['workers'],
            folder_size= input_config['size'],
            delimeter= os.sep,
            gid_col = input_config['gid_col'],
            )
    
    run_download(params)


# call main function
if __name__ == '__main__':
    run()