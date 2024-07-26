from inext_cli.classes.push import push
from inext_cli.classes.sample_sheet import sample_sheet
from inext_cli.classes.config import config, analysisDir
from inext_cli.classes.connect import conf_connect
import json
import os
import logging
from inext_cli.version import __version__
from argparse import (ArgumentParser, ArgumentDefaultsHelpFormatter, RawDescriptionHelpFormatter)
from multiprocessing import Pool, cpu_count
import pathlib
import sys

def add_args(parser=None):
    if parser is None:
        parser = ArgumentParser(
            description="IRIDA Next Command line interface: Push",)
    parser.add_argument('-i','--input', type=str, required=True,help='Sample sheet (tsv, txt, csv, xls, xlsx supported)')
    parser.add_argument('-c', '--config', type=str, required=False, help='Configuration file (json)')
    parser.add_argument('-o', '--outdir', type=str, required=True, help='Output directory to put results')
    parser.add_argument('-u', '--url', type=str, required=False, help='graphql api url')
    parser.add_argument('-t', '--token', type=str, required=False, help='API token')
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument('-p', '--project_code', type=str, required=False, help='IRIDA Next persistent project ID')
    group.add_argument("--proj_col", type=str, required=False, help="Column containing IRIDA Next persistent project ID")
    parser.add_argument("--id_col", type=str, required=False, help="Column containing IRIDA Next persistent project ID or sample name")
    parser.add_argument("--metadata_cols", type=str, required=False, help="Column names which are to be used select specific fields to upload, comma delimetted",default='')
    parser.add_argument("--file_cols", type=str, required=False, help="Column names which contain file paths, combine multiple with comma delimeter",default='')
    parser.add_argument("--skip_rows", type=int, required=False, help="Skip this many rows before the header line",default=0)
    parser.add_argument('--ignore_empty', required=False, help='Do not blank empty metadata fields in IRIDA Next',
                        action='store_true')
    parser.add_argument('--create', required=False, help='Create samples based on sample name',
                        action='store_true')
    parser.add_argument('--skip_meta', required=False, help='Do not write sample metadata to records',
                        action='store_true')
    parser.add_argument("--batch_size", type=int, required=False, help="Max number of id's to query at once",default=1)
    parser.add_argument("--n_records", type=int, required=False, help="Max number of records to request at once",default=100)
    parser.add_argument("--workers", type=int, required=False, help="Number of workers to split the task into",default=1)
    parser.add_argument('-V', '--version', action='version', version="%(prog)s " + __version__)
    return parser

def run_push(run_config):
    if run_config.create and run_config.project_code == '' and run_config.project_col == '':
        logging.critical("Error create mode is enabled without a project code specified or column")
        return
    
    if run_config.access.token == '':
        logging.critical("Error no authentication token provided")
        return

    if run_config.access.url == '':
        logging.critical("Error no graphql url specified")
        return

    if run_config.id_col == '':
        logging.critical("Error no id_col parameter specified")
        return


    pObj = push(run_config)
    if pObj.status:
        logging.info("Uploaded {} records successfully".format(len(pObj.sample_ids)))
    else:
        logging.error("Warning there was an error uploading your data, please check the error log")

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
    num_workers = input_config['workers']
    outputs = analysisDir(baseDir=analysis_parameters['outdir'],
                          dataDir=os.path.join(analysis_parameters['outdir'],"data"),
                          logFile=os.path.join(analysis_parameters['outdir'],"run.log"),
                          errorFile=os.path.join(analysis_parameters['outdir'],"err.log"),
                          resultsFile=os.path.join(analysis_parameters['outdir'],"samples.tsv")
    )
    
    #ignore parameters in config file
    for p in analysis_parameters:
        if p not in input_config:
            input_config[p] = analysis_parameters[p]

    url = analysis_parameters['url']
    if 'url' in input_config and input_config['url'] != '':
        url = input_config['url']

    token = analysis_parameters['token']
    if 'token' in input_config and input_config['token'] != '':
        token = input_config['token']

    id_col = ''
    if 'id_col' in input_config and input_config['id_col'] != '':
        id_col = input_config['id_col']

    metadata_cols = []
    if 'meta_cols' in input_config and input_config['metadata_cols'] != '':
        metadata_cols = input_config['metadata_cols'].split(',')
        
    file_cols = []
    if 'file_cols' in input_config and input_config['file_cols'] != '':
        file_cols = input_config['file_cols'].split(',')

    skip_rows = 0
    if 'skip_rows' in input_config:
        skip_rows = input_config['skip_rows']

    skip_meta = False
    if 'skip_meta' in input_config:
        skip_meta = input_config['skip_meta']
    
    ignore_empty = True
    if 'ignore_empty' in input_config:
        ignore_empty = input_config['ignore_empty']
        
    project_code = ''
    if 'project_code' in input_config and input_config['project_code'] is not None:
        project_code = input_config['project_code']

    project_col = ''
    if 'project_col' in input_config and input_config['project_col'] is not None:
        project_col = input_config['project_col']
    
    create = False
    if 'create' in input_config and input_config['create']:
        create = True

    ss = sample_sheet(sample_sheet=input_config['input'],id_col= id_col, 
                          metadata_cols=metadata_cols, file_cols=file_cols,
                          skip_rows=skip_rows,restrict=skip_meta)
    if not ss.status:
        return

    batch_size = 1
    if 'batch_size' in input_config and input_config['batch_size'] != '':
        batch_size = input_config['batch_size']
    n_cursors = 1
    if 'n_cursors' in input_config and input_config['n_cursors'] != '':
        n_cursors = input_config['n_cursors']


    n_query_ids = len(ss.df)
    if batch_size >  n_query_ids:
        batch_size = n_query_ids


    if not os.path.isdir(outputs.baseDir):
        os.makedirs(outputs.baseDir, 0o755)
    
    if not os.path.isdir(outputs.dataDir):
        os.makedirs(outputs.dataDir, 0o755)

    try:
        sys_num_cpus = len(os.sched_getaffinity(0))
    except AttributeError:
        sys_num_cpus = cpu_count()

    if num_workers > sys_num_cpus:
        num_workers = sys_num_cpus

    worker_num_records = int(n_query_ids / num_workers)
    if num_workers == 1:
        worker_num_records = n_query_ids   
    elif worker_num_records < 1:
        worker_num_records = 1
    list_of_dfs = [ss.df.loc[i:i+worker_num_records-1,:] for i in range(0, len(ss.df),worker_num_records)]
    pool = Pool(processes=num_workers)
    results = []
    output_files = []
    for idx, df in enumerate(list_of_dfs):
        worker_dir = os.path.join(analysis_parameters['outdir'],"worker-{}".format(idx))
        worker_df_path = os.path.join(worker_dir,"worker-subset-{}.{}".format(idx,'tsv'))
        worker_outputs = analysisDir(baseDir=worker_dir,
                        dataDir=os.path.join(worker_dir,"data"),
                        logFile=os.path.join(worker_dir,"run.log"),
                        errorFile=os.path.join(worker_dir,"err.log"),
                        resultsFile=os.path.join(worker_dir,"samples.tsv"))
        output_files.append(worker_outputs)       
        if not os.path.isdir(outputs.baseDir):
            os.makedirs(outputs.baseDir, 0o755)

        if not os.path.isdir(worker_outputs.dataDir):
            os.makedirs(worker_outputs.dataDir, 0o755)

        df.to_csv(worker_df_path,sep="\t",header=True,index=False)

        run_config = config(input_path=worker_df_path, 
                        access =  conf_connect(url=url,token=token),
                        outputs=worker_outputs,
                        id_type = 'sample_puid',
                        ignore_empty=ignore_empty,
                        metadata_cols=metadata_cols,
                        file_cols=file_cols,
                        skip_meta=skip_meta,
                        skip_rows=skip_rows,
                        create=create,
                        project_code=project_code,
                        project_col=project_col,
                        id_col= id_col,
                        n_records = batch_size,
                        n_cursors=n_cursors,
                        batch_size = batch_size
                        )
        results.append(pool.apply_async(run_push, (run_config, )))
    
    pool.close()
    pool.join()
    sys.stdout.flush()

    #retrieve results
    r = []
    for x in results:
        r.append(x.get())

# call main function
if __name__ == '__main__':
    run()