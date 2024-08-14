import logging
import os
from inext_cli.classes.sendFile import sendFile
from inext_cli.classes.sample_sheet import sample_sheet
from inext_cli.utils import validate_connection
from inext_cli.classes.query import query_constructor
from inext_cli.classes.connect import gql_request
import time
import pandas as pd

class push:
    status = True
    def __init__(self, config):
        self.config= config
        self.file_cols = self.config.file_cols
        self.metadata_cols = self.config.metadata_cols
        self.project_code = self.config.project_code
        self.project_col = self.config.project_col
        self.create = self.config.create
        self.id_col = self.config.id_col
        self.skip_meta = self.config.skip_meta
        self.log_fh = open(self.config.outputs.logFile,'w')
        self.err_fh = open(self.config.outputs.errorFile,'w')
        ss = sample_sheet(sample_sheet=self.config.input_path,id_col=self.config.id_col, 
                          metadata_cols=self.config.metadata_cols, file_cols=self.config.file_cols,
                          skip_rows=self.config.skip_rows,restrict=self.skip_meta)
        self.status = ss.status
        self.df = ss.df
        self.wait_time = 1
        self.batch_size = self.config.batch_size
        if not self.status:
            return
        self.query_builder = query_constructor()
        df_cols = list(self.df.columns)

        if self.config.project_col != '':
            if self.config.project_col not in list(self.df.columns):
                logging.critical("Error {} project column was specified but it does not exist in sample sheet columns".
                                 format(self.config.project_col))
                self.status = False
                return
        
        if len(self.metadata_cols) == 0 and not self.config.skip_meta:
            metadata_cols = df_cols
            metadata_cols = self.filter_list(metadata_cols,[self.id_col])
            metadata_cols = self.filter_list(metadata_cols,[self.project_col])
            metadata_cols = self.filter_list(metadata_cols,self.file_cols)
            self.metadata_cols = metadata_cols

        if not self.status:
            return

        self.sample_ids = list(self.df[self.id_col])

        
        if self.project_col != '':
            self.projects = list(self.df[self.project_col])
        else:
            self.projects = [self.project_code] * len(self.sample_ids)
        
        if not validate_connection(self.config):
            self.status = False
            return      
         
        if self.create:
            if self.project_col == '' and self.project_code == '':
                logging.critical("You did not specify a project code and project column, at least one must be set when creating samples")
                self.status = False
                return  
            

        self.run()
        self.err_fh.close()
        self.log_fh.close()


    def get_unique(self,column):
        counts = dict(self.df[column].value_counts())
        u = []
        for k in counts:
            if counts[k] == 1:
                u.append(k)
        return u


    def filter_list(self,l1,l2):
        l2 = set(l2)
        f = []
        for idx,value in enumerate(l1):
            if value in l2:
                continue
            f.append(value)
        return f

    def split_list(self,lst,n):
        return [lst[i:i + n] for i in range(0, len(lst), n)]

    def parse_create_samples(self,data,response):
        for qname in response:
            errors = response[qname]['errors']
            if 'sample' not in response[qname] or not isinstance(response[qname]['sample'],dict):
                puid = ''
                name = qname
                status = False
                
            else:
                puid = response[qname]['sample']['puid']
                name = response[qname]['sample']['name']
                status = True

            data[qname]['puid'] = puid
            data[qname]['name'] = name
            data[qname]['status'] = status
            data[qname]['errors'] = errors

                    
        return data

    def get_error_samples(self,data):
        errors = []
        for id in data:
            if len(data[id]['errors']) != 0:
                errors.append(data[id]['errors'])
        return errors

    def create_samples(self,data={}):
        sample_ids = []
        project_ids = []
        query_names = list(data.keys())
        for qid in query_names:
            sample_ids.append(data[qid]['name'])
            project_ids.append(data[qid]['project_puid'])
        batch_samples = self.split_list(sample_ids,self.batch_size)
        batch_projects = self.split_list(project_ids, self.batch_size)
        batch_query_names = self.split_list(query_names,self.batch_size)



        for idx,bs in enumerate(batch_samples):
            query = self.query_builder.createSamples(query_names=batch_query_names[idx], sample_ids=bs,project_puids=batch_projects[idx])
            query = self.query_builder.render(query)
            r = gql_request(self.config)
            response = r.request(query)
            data = self.parse_create_samples(data,response=response.response)

        return data

    def prep_metadata(self,data={}):
        for idx,row in self.df.iterrows():
            qname = f'idx_{idx}'
            sample_id = row[self.id_col]
            project_id = self.projects[idx]
            metadata = {}
            for field in self.metadata_cols:
                metadata[field] = row[field]
            attachments = []
            for field in self.file_cols:
                attachments.append(row[field])
            data[qname] = {
                'name':sample_id,
                'puid':sample_id,
                'project_puid':project_id,
                'status':True,
                'errors':[],
                'metadata':metadata,
                'attachments':attachments
            }
        return data

    def upload_metadata(self,data):
        query_names = []
        sample_ids = []
        metadata = []
        for qname in data:
            if len(data[qname]['metadata']) == 0:
                continue
            query_names.append(qname)
            sample_ids.append(data[qname]['puid'])
            m = []
            for k,v in data[qname]['metadata'].items():
                m.append(f'{k}:"{v}"')
            metadata.append(",".join(m))
            if len(query_names) == self.batch_size:
                r = gql_request(self.config)
                query = self.query_builder.updateSamples(query_names, sample_ids, metadata)
                query = self.query_builder.render(query)
                response = r.request(query)
                query_names = []
                sample_ids = []
                metadata = []

        if len(query_names) > 0:
            r = gql_request(self.config)
            query = self.query_builder.updateSamples(query_names, sample_ids, metadata)
            query = self.query_builder.render(query)
            response = r.request(query)

    def upload_files(self,data):
        payload = {}
        for qname in data:
            puid = data[qname]['puid']
            files = data[qname]['attachments']
            if len(files) == 0:
                continue
            payload[puid] = files
            if len(payload) == self.batch_size:
                send_obj = sendFile(self.config,payload)
                send_obj.run()
                payload = {}
        if len(payload) > 0:
            send_obj = sendFile(self.config,payload)
            send_obj.run()
    

    def run(self):
        data = self.prep_metadata()
        if self.create:
            data = self.create_samples(data)
        if not self.skip_meta:
            self.upload_metadata(data)

        if self.file_cols != '':
            self.upload_files(data)

            