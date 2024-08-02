from inext_cli.classes.pull import pull
from inext_cli.classes.sample_sheet import sample_sheet
from inext_cli.classes.config import config, analysisDir
from inext_cli.classes.connect import conf_connect
from inext_cli.utils import concatonate_files, file_valid, get_line_count
from dataclasses import asdict
import json
import os
import logging
from inext_cli.version import __version__
from argparse import (ArgumentParser, ArgumentDefaultsHelpFormatter, RawDescriptionHelpFormatter)
from multiprocessing import Pool, cpu_count
import pathlib
import sys
import shutil

def add_args(parser=None):
    if parser is None:
        parser = ArgumentParser(
            description="IRIDA Next Command line interface: Pull",)
    parser.add_argument('-i','--input', type=str, required=True,help='Sample sheet (tsv, txt, csv, xls, xlsx supported)')
    parser.add_argument('-c', '--config', type=str, required=False, help='Configuration file (json)')
    parser.add_argument('-o', '--outdir', type=str, required=True, help='Output directory to put results')
    parser.add_argument('-u', '--url', type=str, required=False, help='graphql api url')
    parser.add_argument('-t', '--token', type=str, required=False, help='API token')
    parser.add_argument("--id_col", type=str, required=False, help="Column containing IRIDA Next persistent project ID or sample name")
    parser.add_argument("--id_type", type=str, required=False, help="specify what type of id is in the sample sheet (sample, group, project, user)",default='sample')
    parser.add_argument("--file_type", type=str, required=False, help="Retrieve this type of file if download is toggled (all, fasta, fastq, json, text, csv, tsv, genbank)",default='all')
    parser.add_argument("--skip_rows", type=int, required=False, help="Skip this many rows before the header line",default=0)
    parser.add_argument('--download', required=False, help='Create samples based on sample name',
                        action='store_true')
    parser.add_argument("--workers", type=int, required=False, help="Number of workers to split the task into",default=1)
    parser.add_argument('-V', '--version', action='version', version="%(prog)s " + __version__)
    return parser




def run_pull(run_config,summary=False):
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


    pObj = pull(run_config)
    if not summary:
        pObj.retrieve_all()
        if pObj.status:
            logging.info("Queried {} records successfully".format(len(pObj.sample_data)))
        else:
            logging.error("Warning there was an error querying your data, please check the error log")
            pass




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

    
    outputs = analysisDir(baseDir=analysis_parameters['outdir'],
                          dataDir=os.path.join(analysis_parameters['outdir'],"data"),
                          logFile=os.path.join(analysis_parameters['outdir'],"run.log"),
                          errorFile=os.path.join(analysis_parameters['outdir'],"err.log"),
                          resultsFile=os.path.join(analysis_parameters['outdir'],"samples.tsv"),
                        groupIndexFile=os.path.join(analysis_parameters['outdir'],"groupIndex.tsv"),
                        projectIndexFile=os.path.join(analysis_parameters['outdir'],"projectIndex.tsv"),
                        sampleIndexFile=os.path.join(analysis_parameters['outdir'],"samplesIndex.tsv"),
                        attachIndexFile=os.path.join(analysis_parameters['outdir'],"attachIndex.tsv"),       
    )
    
    #ignore parameters in config file
    for p in analysis_parameters:
        if p not in input_config:
            input_config[p] = analysis_parameters[p]
    num_workers = input_config['workers']
    id_col = analysis_parameters['id_col']
    if 'id_col' in input_config and input_config['id_col'] is not '':
        id_col = input_config['id_col']
    
    if id_col is None:
        print("Error you need to specify the header name for your identifier")
        sys.exit()

    id_type = analysis_parameters['id_type']
    if 'id_type' in input_config and input_config['id_type'] is not '':
        id_type = input_config['id_type']

    skip_rows = 0
    if 'skip_rows' in input_config:
        skip_rows = input_config['skip_rows']

    url = analysis_parameters['url']
    if 'url' in input_config and input_config['url'] != '':
        url = input_config['url']

    token = analysis_parameters['token']
    if 'token' in input_config and input_config['token'] != '':
        token = input_config['token']


    ss = sample_sheet(sample_sheet=input_config['input'],id_col=id_col, 
                          metadata_cols=[], file_cols=[],
                          skip_rows=skip_rows,restrict=True)

    if ss.status == False:
        return
    
    if not os.path.isdir(outputs.baseDir):
        os.makedirs(outputs.baseDir, 0o755)
    
    if not os.path.isdir(outputs.dataDir):
        os.makedirs(outputs.dataDir, 0o755)

    try:
        sys_num_cpus = len(os.sched_getaffinity(0))
    except AttributeError:
        sys_num_cpus = cpu_count()

    print(sys_num_cpus)
    if num_workers > sys_num_cpus:
        num_workers = sys_num_cpus
    print(num_workers)

    batch_size = 1
    if 'batch_size' in input_config and input_config['batch_size'] != '':
        batch_size = input_config['batch_size']
    
    n_cursors = 1
    if 'n_cursors' in input_config and input_config['n_cursors'] != '':
        n_cursors = input_config['n_cursors']


    n_query_ids = len(ss.df)
    if batch_size >  n_query_ids:
        batch_size = n_query_ids

    worker_num_records = int(n_query_ids / num_workers)
    if num_workers == 1:
        worker_num_records = n_query_ids   
    elif worker_num_records < 1:
        worker_num_records = 1
    

    project_col = None
    if project_col in input_config and input_config['project_col'] != '':
        project_col = input_config['project_col']

    list_of_dfs = [ss.df.loc[i:i+worker_num_records-1,:] for i in range(0, n_query_ids,worker_num_records)]
    pool = Pool(processes=num_workers)
    results = []
    worker_files = []
    for idx, df in enumerate(list_of_dfs):
        worker_dir = os.path.join(analysis_parameters['outdir'],"worker-{}".format(idx))
        worker_df_path = os.path.join(worker_dir,"worker-subset-{}.{}".format(idx,'tsv'))
        worker_outputs = analysisDir(baseDir=worker_dir,
                        dataDir=os.path.join(worker_dir,"data"),
                        logFile=os.path.join(worker_dir,"run.log"),
                        errorFile=os.path.join(worker_dir,"err.log"),
                        resultsFile=os.path.join(worker_dir,"results.tsv"),
                        groupIndexFile=os.path.join(worker_dir,"groupIndex.tsv"),
                        projectIndexFile=os.path.join(worker_dir,"projectIndex.tsv"),
                        sampleIndexFile=os.path.join(worker_dir,"samplesIndex.tsv"),
                        attachIndexFile=os.path.join(worker_dir,"attachIndex.tsv"),                                         
                        )    
        worker_files.append(worker_outputs)   
        if not os.path.isdir(outputs.baseDir):
            os.makedirs(outputs.baseDir, 0o755)

        if not os.path.isdir(worker_outputs.dataDir):
            os.makedirs(worker_outputs.dataDir, 0o755)

        df.to_csv(worker_df_path,sep="\t",header=True,index=False)
        

        run_config = config(input_path=worker_df_path, 
                        id_col= id_col,
                        id_type=id_type,
                        access = conf_connect(url=url,token=token),
                        outputs=worker_outputs,
                        skip_rows=skip_rows,
                        n_records = batch_size,
                        n_threads = num_workers,
                        n_cursors=n_cursors,
                        skip_meta=True,
                        create=False,                                              
                        )
        results.append(pool.apply_async(run_pull, (run_config, )))
    
    pool.close()
    pool.join()
    sys.stdout.flush()

    #retrieve results
    r = []
    for x in results:
        r.append(x.get())

    #merge results
    files = {'groupIndexFile':[],'projectIndexFile':[],'sampleIndexFile':[],'attachIndexFile':[]}
    for worker_outputs in worker_files:
        worker_outputs = asdict(worker_outputs)
        for rtype in files:
            f = worker_outputs[rtype]
            if file_valid(f) and get_line_count(f) > 2:
                files[rtype].append(f)

    out_files = asdict(outputs)       
    for rtype in files:
        df = concatonate_files(files[rtype])
        if len(df) > 0:
            df.to_csv(out_files[rtype],sep="\t",header=True, index=False)

    #clean up 
    for worker_outputs in worker_files:
        baseDir = worker_outputs.baseDir 
        if os.path.isdir(baseDir):
            shutil.rmtree(worker_outputs.baseDir)


# call main function
if __name__ == '__main__':
    run()