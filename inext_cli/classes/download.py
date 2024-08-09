import requests
import pandas as pd
import sys
import os
import re
from multiprocessing import Pool, cpu_count
from inext_cli.utils import file_valid,get_line_count
from dataclasses import dataclass
from inext_cli.classes.query import query_constructor
from inext_cli.classes.connect import gql_request

@dataclass
class download_params:
    file_path: str
    sample_col: str
    filename_col: str
    sub_folder_col: str
    url_col: str
    gid_col: str 
    baseDir: str
    manifestFile:str
    logFile: str
    errorFile: str
    gql_config: dataclass
    file_types: str = "."
    n_workers: int = 1
    folder_size: int = 5000
    delimeter: str = '/'



class download:
    def __init__(self,download_params) -> None:
        self.config = download_params.gql_config
        self.query_builder = query_constructor()
        self.params = download_params
        baseDir = download_params.baseDir
        if not os.path.isdir(baseDir):
            os.makedirs(baseDir, 0o755)
        try:
            sys_num_cpus = len(os.sched_getaffinity(0))
        except AttributeError:
            sys_num_cpus = cpu_count()

        n_workers = self.params.n_workers
        if n_workers > sys_num_cpus:
            n_workers = sys_num_cpus
        self.n_workers = n_workers

        self.df = self.read_file(self.params.file_path)
        self.df = self.regex_filter_df(self.df,self.params.filename_col,self.params.file_types)
        if len(self.df) == 0:
            return
        self.df['batch'] = self.set_batch_num(self.params.sample_col,self.params.folder_size)
        self.df['batch'] = self.df['batch'].astype(str)
        self.df['folder'] = self.df[['batch', self.params.sample_col, self.params.sub_folder_col]].apply(lambda row: os.sep.join(row.values.astype(str)), axis=1)
        self.subsets = self.subset_df(self.df,self.params.sample_col,self.n_workers)
        self.run()

    def regex_filter_df(self,df,colname,regex_str):
        include = []
        for idx,row in df.iterrows():
            if re.search(regex_str,row[colname]):
                include.append(idx)
        return df[df.index.isin(include)]

    def set_batch_num(self,id_col,size):
        samples = list(self.df[id_col].unique())
        batches = self.split_list(samples,size)
        for idx,b in enumerate(batches):
            batches[idx] = set(b)
        batch_id = []
        for idx,row in self.df.iterrows():
            sample_id = row[id_col]
            for bid, batch in enumerate(batches):
                if sample_id in batch:
                    batch_id.append(bid)
        return batch_id

    def subset_df(self,df,id_col,n_workers=1):
        samples = list(self.df[id_col].unique())
        batches = self.split_list(samples,n_workers)
        subsets = {}
        for group_id,batch  in enumerate(batches):
            subsets[group_id] = df[df[id_col].isin(batch)]
        return subsets


    def split_list(self,lst,n):
        return [lst[i:i + n] for i in range(0, len(lst), n)]

    def download_file(self, url: str, filename: str):
        with open(filename, 'wb') as f:
            with requests.get(url, stream=True) as r:
                r.raise_for_status()
                for chunk in r.iter_content(chunk_size=81920):
                    f.write(chunk)

    def setup_download_dir(self,baseDir,df):
        if not os.path.isdir(baseDir):
            os.makedirs(baseDir, 0o755)
        folders_to_create = list(df['folder'].unique())
        for f in folders_to_create:
            try:
                if not os.path.isdir(os.path.join(baseDir,f)):
                    os.makedirs(os.path.join(baseDir,f), 0o755)
            except Exception:
                print(f"Couldn't create folder: {os.path.join(baseDir,f)}")

    def process_batch(self,baseDir,df,url_col,filename_col,gid_col):
        self.setup_download_dir(baseDir,df)
        response = {}
        for idx,row in df.iterrows():
            url = row[url_col]
            folder_path = os.path.join(baseDir,row['folder'])
            if not os.path.isdir(baseDir):
                os.makedirs(folder_path, 0o755)
            filename = os.path.join(folder_path,row[filename_col])
            try:
                self.download_file(url, filename)
                response[idx] = {'status':True,'error':''}
            except Exception as e:
                if gid_col is not None and gid_col != '':
                    try:
                        id = row[gid_col]
                        url = self.fetch_url(id)
                        self.download_file(url, filename)
                        response[idx] = {'status':True,'error':''}
                    except Exception as e:
                        response[idx] = {'status':False,'error':e}
                     
                else:
                    response[idx] = {'status':False,'error':e}
        return response

    def fetch_url(self,id):
        r = gql_request(self.config)
        query = self.query_builder.queryAttachements(['idx'],[id])
        query = self.query_builder.render(query)
        response = r.request(query)
        errors = response.errors
        if len(errors) == 0:
            return response.response['idx']['attachmentUrl']
        return ''

    def read_file(self,f):
         if file_valid(f) and get_line_count(f) > 1:
             return pd.read_csv(f, header=0, sep="\t", dtype=str)
         else:
             return pd.DataFrame()


    def run(self):
        #prevent race condition on root folders
        baseDir = self.params.baseDir
        if not os.path.isdir(baseDir):
            os.makedirs(baseDir, 0o755)

        batches = list(self.df['batch'].unique())
        for b in batches:
            if not os.path.isdir(os.path.join(baseDir,b)):
                os.makedirs(os.path.join(baseDir,b), 0o755)

        #download multithreaded
        pool = Pool(processes=self.n_workers)
        results = []
        for id in self.subsets:
            results.append(pool.apply_async(self.process_batch, (baseDir, self.subsets[id],self.params.url_col,self.params.filename_col, self.params.gid_col)))

        pool.close()
        pool.join()
        sys.stdout.flush()

        #retrieve results
        r = []
        for x in results:
            r.append(x.get())
        
        dfs = []
        for idx,df in enumerate(self.subsets):
            response = r[idx]
            status = []
            error = []
            for idy in response:
                status.append(response[idy]['status'])
                error.append(response[idy]['error'])
            df['download_status'] = status
            df['download_error_msg'] = error
            dfs.append(df)
        
        pd.concat(dfs, axis=0).to_csv(self.params.manifestFile,sep="\t",index=False,header=True)



