import logging
from inext_cli.classes.sample_sheet import sample_sheet
from inext_cli.classes.query import query_constructor
from inext_cli.utils import validate_connection
from inext_cli.classes.connect import gql_request
from inext_cli.classes.download import download, download_params
import pandas as pd
from gql.transport.exceptions import TransportQueryError
import os
import re
import requests
from multiprocessing import Pool, cpu_count

class pull:
    status = True
    user_data = {'user_id': '', 'user_email': ''}
    sample_data = {}
    project_data = {}
    group_data = {}

    def __init__(self, config):
        self.config= config
        self.id_col = self.config.id_col
        self.id_type = self.config.id_type
        self.project_col = self.config.project_col
        self.file_regex = self.config.file_regex
        self.first = self.config.n_cursors
        self.batch_size = self.first
        self.wait_time = self.config.wait_time
        self.timeout = self.config.timeout
        self.extensions = self.config.file_regex.extensions
        self.file_types = self.config.file_regex.file_types
        self.log_fh = open(self.config.outputs.logFile,'w')
        self.err_fh = open(self.config.outputs.errorFile,'w')
        self.query_builder = query_constructor()
        self.skip_meta = self.config.skip_meta
        self.download = self.config.download
        self.download_dir = self.config.outputs.dataDir
        self.download_regex = self.config.download_regex
        self.download_workers = self.config.download_workers
        
    
        ss = sample_sheet(sample_sheet=self.config.input_path,id_col=self.config.id_col, 
                          metadata_cols=self.config.metadata_cols, file_cols=self.config.file_cols,
                          skip_rows=self.config.skip_rows,restrict=self.skip_meta)
        self.status = ss.status
        if not self.status:
            return
        self.query_ids = list(set(ss.df[self.id_col]))
    
        if not validate_connection(self.config):
            self.status = False
            return      
        project_puids=[]
        if self.project_col != '':
            project_puids = list(set(ss.df[self.project_col]))
        self.project_puids = project_puids
    
    def retrieve_all(self):
        self.run(ids=self.query_ids ,id_type=self.id_type,first=self.first,project_puids=self.project_puids)
        self.err_fh.close()
        self.log_fh.close()
    
    def retrieve_summary(self):
        pass

    def get_sample_metadata_keys(self):
        fields = set()
        for puid in self.sample_data:
            if 'metadata' in self.sample_data[puid]:
                fields = fields | self.sample_data[puid]['metadata']
        return fields

    def format_sample_metadata(self):
        fields = sorted(list(self.get_sample_metadata_keys))
        self.sample_metadata = {}
        for puid in self.sample_data:
            sample = self.sample_data[puid]
            row = {}
            metadata = {}
            if 'metadata' in sample:
                metadata = sample['metadata']
            for f in fields:
                row[f] = ''
                if f in metadata:
                    row[f] = metadata[f]
            self.sample_data[puid] = metadata

    def parse_sample_response(self,data,response,id_type):
        for qname in response:
            puid = qname
            if id_type == 'name':
                puid = response['puid']
            if qname not in data:
                data[puid] = {
                    'puid':response[qname]['puid'],
                    'id':response[qname]['id'],
                    'name':response[qname]['name'],
                    'description':response[qname]['description'],
                    'project':response[qname]['project']['puid'],
                    'metadata':response[qname]['metadata'],
                    'attachments':{},
                    'hasNextPage':True,
                    'cursor':''
                }
                
            data[qname]['hasNextPage'] = response[qname]['attachments']['pageInfo']['hasNextPage']
            data[qname]['cursor'] = response[qname]['attachments']['pageInfo']['endCursor']


            for node in response[qname]['attachments']['nodes']:
                id = node['id']
                data[qname]['attachments'][id] = {
                    'attachmentUrl':node['attachmentUrl'],
                    'byteSize':node['byteSize'],
                    'createdAt':node['createdAt'],
                    'filename':node['filename'],
                    'id':node['id'],
                    'metadata':node['metadata'],
                    'puid':node['puid']
                }


        return data

    def get_samples_by_group(self,query_names,cursors,ids,first):
        r = gql_request(self.config)
        query = self.query_builder.queryGroupSamples(query_names,cursors,ids=ids,first=first)
        query = self.query_builder.render(query)
        return r.request(query)
    
    def get_samples_by_namespace(self,namespaceType, query_names,cursors,puids,first):
        r = gql_request(self.config)
        query = self.query_builder.queryNamespaces(namespaceType, query_names,cursors,puids=puids,first=first)
        query = self.query_builder.render(query)
        return r.request(query)

    def get_sample_data(self,id_type,query_names,cursors,sample_ids,project_puids,first):      
        query = self.query_builder.querySamples(id_type,query_names,cursors,sample_ids,project_puids=project_puids,first=first)
        query = self.query_builder.render(query)
        r = gql_request(self.config)
        return r.request(query)
    

    def split_list(self,lst,n):
        return [lst[i:i + n] for i in range(0, len(lst), n)]

    def find_sample_batch_size(self,id_type,query_names,project_puids,sample_ids,first=1):
        batch_size = self.batch_size
        #lowering the number of cursors is less efficient than shrinking the number of records
        batch_size = self.batch_size
        query_name_batches = self.split_list(query_names,batch_size)
        sample_batches = self.split_list(sample_ids,batch_size)
        if len(project_puids) > 0:
            project_batches = self.split_list(project_puids,batch_size)
        else:
            project_batches = [[]]*len(sample_batches)

        qb = query_name_batches[0]
        sb = sample_batches[0]
        pb = project_batches[0]
        cb = ['']*len(qb)
        response = self.get_sample_data(id_type=id_type,query_names=qb,cursors=cb,
                                        sample_ids=sb,project_puids=pb,first=first)
        errors = response.errors
        num_records = len(qb)
        tryAgain = False
        if len(errors) > 0:
            tryAgain = self.errorHandler(errors)
        
        if first > batch_size:
            first = batch_size
            self.first = first
        if self.first < 1:
            self.first = 1
        while tryAgain:
            if self.first == 1 and num_records == 1:
                break
            first = self.first
            response = self.get_sample_data(id_type=id_type,query_names=qb,cursors=cb,
                                        sample_ids=sb,project_puids=pb,first=first)
            errors = response.errors
            if len(errors) > 0:
                tryAgain = self.errorHandler(errors)
            else:
                tryAgain = False
                break
            
            num_records = int(len(qb) /2)
            self.first = int(self.first /2)
            if self.first < 1:
                self.first = 1
            if num_records < 1:
                break

            qb = qb[0:num_records+1]
            sb = sb[0:num_records+1]
            pb = pb[0:num_records+1]
            cb = cb[0:num_records+1]

        self.batch_size = num_records

        if len(errors) == 0:
            return True

        return False

    def retrieve_sample_data(self,id_type,sample_ids,project_puids=[],first=10):
        #invalid, need both name and project
        if id_type == 'sample_name' and len(project_puids) == 0:
            return
        query_names = sample_ids
        if id_type == 'sample_name':
            query_names = []
            for idx,value in enumerate(query_names):
                query_names.append(f'idx_{idx}')
        data = {}
        self.first = 10
        #No batch size could satisfy the query complexity
        if not self.find_sample_batch_size(id_type,query_names,project_puids,sample_ids,first):
            print(f"No batch size worked: batch:{self.batch_size}, cursors:{self.first}")
            return data
        
        first = self.first
        batch_size = self.batch_size
        query_name_batches = self.split_list(query_names,batch_size)
        sample_batches = self.split_list(sample_ids,batch_size)

        if len(project_puids) > 0:
            project_batches = self.split_list(project_puids,batch_size)
        else:
            project_batches = [[]]*len(sample_batches)

        cursors = [[]]*len(sample_batches)
        for idx,sb in enumerate(sample_batches):
            cursors[idx] = ['']*len(sb)
        


        for idx,qb in enumerate(query_name_batches):
            sb = sample_batches[idx]
            pb = project_batches[idx]
            cb = cursors[idx]
            response = self.get_sample_data(id_type=id_type,query_names=qb,cursors=cb,
                                            sample_ids=sb,project_puids=pb,first=first)
            errors = response.errors
            if len(errors) > 0:
                return data

            data = self.parse_sample_response(data=data,response=response.response,id_type=id_type)
            pageInfo = self.get_pagination(data=data)
            while(len(pageInfo['puids']) > 0):
                response = self.get_sample_data(id_type=id_type,query_names=pageInfo['puids'],cursors=pageInfo['cursors'],
                                    sample_ids=sb,project_puids=pb,first=first)
                errors = response.errors
                if len(errors) > 0:
                    return data
                data = self.parse_sample_response(data=data,response=response.response,id_type=id_type)
                pageInfo = self.get_pagination(data=data)
        pd.DataFrame.from_dict(data,orient='index').to_csv(self.config.outputs.sampleIndexFile,sep="\t",header=True,index=False)
        return data

    def parse_group_sample_response(self,data,response):
        for qname in response:
            if response[qname] is None:
                continue
            elif qname not in data:
                data[qname] = {
                    'totalCount':response[qname]['totalCount'],
                    'samples':{},
                    'hasNextPage':True,
                    'cursor':''
                }
                
            data[qname]['hasNextPage'] = response[qname]['pageInfo']['hasNextPage']
            data[qname]['cursor'] = response[qname]['pageInfo']['endCursor']

            for node in response[qname]['nodes']:
                puid = node['puid']
                data[qname]['samples'][puid] = node['id']
        return data

    
    def parse_namespace_response(self,data,response,namespaceType):
        if namespaceType == 'project':
            namespace_key = 'samples'
        else:
            namespace_key = 'projects'
        for qname in response:
            if response[qname] is None:
                continue
            elif qname not in data:
                data[qname] = {
                    'puid':response[qname]['puid'],
                    'id':response[qname]['id'],
                    'name':response[qname]['name'],
                    'fullPath':response[qname]['fullPath'],
                    'fullName':response[qname]['fullName'],
                    'description':response[qname]['description'],
                    'totalCount':response[qname][namespace_key]['totalCount'],
                    namespace_key:set(),
                    'hasNextPage':True,
                    'cursor':''
                }
                
            data[qname]['hasNextPage'] = response[qname][namespace_key]['pageInfo']['hasNextPage']
            data[qname]['cursor'] = response[qname][namespace_key]['pageInfo']['endCursor']

            for node in response[qname][namespace_key]['nodes']:
                puid = node['puid']
                data[qname][namespace_key].add(puid)
        return data

    def get_pagination(self,data):
        query_names = []
        cursors=[]
        for puid in data:
            if data[puid]['hasNextPage']:
                query_names.append(puid)
                cursors.append(data[puid]['cursor'])
        return {'puids':query_names,'cursors':cursors}

    def errorHandler(self,e):
        tryAgain = False
        if 'errors' in e:
            if isinstance(e['errors'],TransportQueryError):
                tryAgain = True
        return tryAgain

    def retrieve_namespace_data(self,puids,first,namespaceType):
        data = {}
        response = self.get_samples_by_namespace(query_names=puids, namespaceType=namespaceType, puids=puids,cursors=['']*len(puids),first=first)
        errors = response.errors
        tryAgain = False
        if len(errors) > 0:
            tryAgain = self.errorHandler(errors)
        while tryAgain:
            first = self.first
            response = self.get_samples_by_namespace(query_names=puids, namespaceType=namespaceType, puids=puids,cursors=['']*len(puids),first=first)
            errors = response.errors
            if len(errors) > 0:
                tryAgain = self.errorHandler(errors)
            else:
                tryAgain = False
        if len(errors) > 0:
            return data
        data = self.parse_namespace_response(data=data,response=response.response,namespaceType=namespaceType)
        pageInfo = self.get_pagination(data=data)

        while(len(pageInfo['puids']) > 0):
            response = self.get_samples_by_namespace(query_names=pageInfo['puids'],namespaceType=namespaceType, puids=pageInfo['puids'],cursors=pageInfo['cursors'],first=first)
            errors = response.errors
            tryAgain = False
            if len(errors) > 0:
                tryAgain = self.errorHandler(errors)
            while tryAgain:
                first = self.first
                response = self.get_samples_by_namespace(query_names=pageInfo['puids'],namespaceType=namespaceType, puids=pageInfo['puids'],cursors=pageInfo['cursors'],first=first)
                errors = response.errors
                if len(errors) > 0:
                    tryAgain = self.errorHandler(errors)
                else:
                    tryAgain = False
            if len(errors) > 0:
                return data  
            data = self.parse_namespace_response(data=data,response=response.response,namespaceType=namespaceType)
            pageInfo = self.get_pagination(data=data)
        return data
    
    def retrieve_group_samples(self,group_puids,group_ids,first=10):
        data = {}
        for idx,group_puid in enumerate(group_puids):
            group_id = group_ids[idx]
            query_names = [group_puid]
            cursors=['']
            response = self.get_samples_by_group(query_names,cursors,[group_id],first)
            errors = response.errors
            if len(errors) > 0:
                return data
            data = self.parse_group_sample_response(data,response.response)
            pageInfo = self.get_pagination(data=data)
            while(len(pageInfo['puids']) > 0):
                cursors=pageInfo['cursors']
                response = self.get_samples_by_group(query_names,cursors,[group_id],first)
                errors = response.errors
                if len(errors) > 0:
                    return data
                data = self.parse_group_sample_response(data,response.response)
                pageInfo = self.get_pagination(data=data)
        return data

    def process_namespace(self,puids,first,namespaceType):
        
        if namespaceType == 'group':
            project_puids = []
        else:
            project_puids = puids

        self.sample_data = {}
        if namespaceType == 'group':
            self.group_data = self.retrieve_namespace_data(puids=puids,first=first,namespaceType=namespaceType)
            group_ids = []
            group_puids = []
            for k in self.group_data:
                group_ids.append(self.group_data[k]['id'])
                group_puids.append(self.group_data[k]['puid'])
            group_samples = self.retrieve_group_samples(group_puids,group_ids,first)
            sample_ids = []
            for group_id in group_samples:
                sample_ids += list(group_samples[group_id]['samples'].keys())
            sample_ids = sorted(list(set(sample_ids)))
            self.sample_data.update(self.retrieve_sample_data(id_type='puid',sample_ids=sample_ids,project_puids=project_puids,first=first))
            project_puids = []
            for sample_puid in self.sample_data:
                project_puids.append(self.sample_data[sample_puid]['project'])
            self.project_data = self.retrieve_namespace_data(puids=list(set(project_puids)),first=first,namespaceType='project')
            pd.DataFrame.from_dict(data={'project_puid':project_puids,'sample_puid':list(self.sample_data.keys())},orient='columns').to_csv(self.config.outputs.projectIndexFile,sep="\t",header=True,index=False)
            
            data = {
                'group_puid':[],
                'project_puid':[],
                'sample_puid':[]
            }
            for group_id in group_samples:
                for sample_puid in group_samples[group_id]['samples']:
                    project_puid = self.sample_data[sample_puid]['project']
                    data['group_puid'].append(group_id)
                    data['project_puid'].append(project_puid)
                    data['sample_puid'].append(sample_puid)
            pd.DataFrame.from_dict(data,orient='columns').to_csv(self.config.outputs.groupIndexFile,sep="\t",header=True,index=False)

        elif namespaceType == 'project':
            self.project_data = self.retrieve_namespace_data(puids=list(project_puids),first=first,namespaceType='project')
            samples = {}
            for project_id in self.project_data:
                for id in self.project_data[project_id]['samples']:
                    samples[id] = project_id

            sample_ids=list(samples.keys())
            project_puids=list(samples.values())
            
            if first != self.first:
                first = self.first
            pd.DataFrame.from_dict(data={'project_puid':project_puids,'sample_puid':sample_ids},orient='columns').to_csv(self.config.outputs.projectIndexFile,sep="\t",header=True,index=False)
        
            self.sample_data.update(self.retrieve_sample_data(id_type='puid',sample_ids=sample_ids,project_puids=project_puids,first=first))


    def process_sample(self,id_type,sample_ids,project_puids,first):
        self.sample_data = self.retrieve_sample_data(id_type=id_type,sample_ids=sample_ids,project_puids=project_puids,first=first)
        puids = set()
        project_puids = []
        for sample_id in self.sample_data:
            project_id = self.sample_data[sample_id]['project']
            project_puids.append(project_id)

            puids.add(project_id)
        self.project_data = self.process_namespace(puids,first,'project')
        if not os.path.isfile(self.config.outputs.projectIndexFile):
            pd.DataFrame.from_dict(data={'project_puid':project_puids,'sample_puid':list(self.sample_data.keys())},orient='columns').to_csv(self.config.outputs.projectIndexFile,sep="\t",header=True,index=False)


    def get_user(self):
        query = self.query_builder.query_user()
        query = self.query_builder.render(query)
        r = gql_request(self.config)
        response = r.request(query)
        self.user_data['id'] = response.response['currentUser']['id']
        self.user_data['email'] = response.response['currentUser']['email']

    def parse_groups(self,data,response):
        for node in response['groups']:
            puid = response['groups']['puid']
            if puid not in data:
                data[puid] = {
                    'puid':puid,
                    'id':response['groups']['id'],
                    'name':response['groups']['name'],
                    'fullPath':response['groups']['fullPath'],
                    'fullName':response['groups']['fullName'],
                    'description':response['groups']['description'],
                    'totalCount':response['nodes']['totalCount'],
                    'projects':set(),
                    'hasNextPage':True,
                    'cursor':''
                }
                
            data[puid]['hasNextPage'] = response['groups']['nodes']['pageInfo']['hasNextPage']
            data[puid]['cursor'] = response['groups']['nodes']['pageInfo']['endCursor']

            for node in response['groups']['nodes']:
                data[puid]['projects'].add(node['puid'])
        return data


    def format_attachment_index(self):
        data = {}
        for sample_id in self.sample_data:
            if 'attachments' not in self.sample_data[sample_id]:
                continue
            for puid in self.sample_data[sample_id]['attachments']:
                r = self.sample_data[sample_id]['attachments'][puid]
                data[puid] = {
                    'attachment_puid':puid,
                    'sample_puid':sample_id,
                    'attachmentUrl':r['attachmentUrl'],
                    'byteSize':r['byteSize'],
                    'createdAt':r['createdAt'],
                    'filename':r['filename'],
                    'id':r['id'],
                    'metadata':r['metadata'],
                    'puid':r['puid']
                }
        return data


    def run(self,ids,id_type,first=10,project_puids=[]):
        self.get_user()
        if id_type == 'sample_puid' or id_type == 'sample_name':
            self.process_sample(id_type=id_type,sample_ids=ids,project_puids=project_puids,first=first)
        elif id_type in ['project','group']:
            self.process_namespace(puids=ids,first=first,namespaceType=id_type)
        elif id_type == 'user':
           #to do
           pass
        else:
            logging.error("You need to specify either id_type equal to user, sample, group, project")      
        pd.DataFrame.from_dict(self.format_attachment_index(),orient='index').to_csv(self.config.outputs.attachIndexFile,sep="\t",header=True,index=False)
