import requests
import sys
import os
import re
import pandas as pd
from inext_cli.classes.sample_sheet import sample_sheet

class download:
    n_workers = 1
    records = {}
    
    def __init__(self, input_path, id_col, file_col, skip_rows=0, file_regex=".",n_threads=1,folder_size=5000):

        self.id_col = id_col
        self.folder_size = folder_size
        self.file_regex = file_regex
        self.n_threads = n_threads
        self.log_fh = open(self.config.outputs.logFile,'w')
        self.err_fh = open(self.config.outputs.errorFile,'w')

        ss = sample_sheet(sample_sheet=self.input_path,id_col=self.id_col, 
                          metadata_cols="", file_cols="",
                          skip_rows=skip_rows,restrict=False)
        self.status = ss.status
        if not self.status:
            return
        self.ids = list(set(ss.df[self.id_col]))
        

    def submit(self):
        pass

    def initDirectories(self):
        pass
    
    def filter_files(self,colname,df,regex_pattern="."):
        filter_indices = []
        for idx,row in df.iterrows():
            f = row[colname]
            if re.search("{}".format(regex_pattern), f) is not None:
                filter_indices.append(idx)
        return df.filter(items=filter_indices, axis=0).reset_index(drop=True)



    def download(self, url: str, filename: str):
        with open(filename, 'wb') as f:
            with requests.get(url, stream=True) as r:
                r.raise_for_status()
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)


