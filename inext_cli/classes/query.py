from gql import gql

class query_strings:
    def __init__(self) -> None:
        pass

    def query_user(self):
        return f'query {{\n currentUser {{ email id }}      }}'  
    
    def query_individual_sample_by_puid(self,puid,after,first):
        return f'sample(puid: "{puid}") {{ createdAt updatedAt description id metadata name puid attachments(first: {first}, after: "{after}") {{ nodes {{ attachmentUrl byteSize createdAt filename id metadata puid }} pageInfo {{ endCursor hasNextPage hasPreviousPage startCursor }} }} project {{ puid }} }}'
    
    def query_individual_sample_by_id(self,id,after,first):
        return f'sample(id: "{id}") {{ createdAt updatedAt description id metadata name puid attachments(first: {first}, after: "{after}") {{ nodes {{ attachmentUrl byteSize createdAt filename id metadata puid }} pageInfo {{ endCursor hasNextPage hasPreviousPage startCursor }} }} project {{ puid }} }}'
        
    def query_individual_sample_by_name(self,puid,sampleName,after,first):
        return f'projectSample(projectPuid: "{puid}", sampleName: "{sampleName}") {{ createdAt updatedAt description id metadata name puid attachments(after: "{after}", first: {first}) {{ totalCount nodes {{ attachmentUrl byteSize createdAt filename id metadata puid }} pageInfo {{ endCursor hasNextPage hasPreviousPage startCursor }} }} project {{ puid }} }}'
    
    def query_samples_by_group_id(self,id,after,first):
        return f'samples(groupId: "{id}", after: "{after}", first: {first}) {{  totalCount nodes {{ id puid }} pageInfo {{ endCursor hasNextPage hasPreviousPage startCursor }}      }} '

    def query_project_by_puid(self,puid,after,first):
        return f'project(puid: "{puid}") {{ createdAt updatedAt description fullName fullPath id name path puid samples(after: "{after}", first: {first}) {{ totalCount nodes {{ id name puid }} pageInfo {{ endCursor hasNextPage hasPreviousPage startCursor }} }} }}'
    
    def query_group_by_puid(self,puid,after,first):
        return f'group(puid: "{puid}") {{ createdAt updatedAt description fullName fullPath id name path puid projects(after: "{after}", first: {first}) {{ totalCount nodes {{ id name puid }} pageInfo {{ endCursor hasNextPage hasPreviousPage startCursor }} }} }}'
    
    def query_sample_attachment(self,id):
        return f'node(id: "{id}") {{ id ... on Attachment {{ attachmentUrl byteSize createdAt filename id metadata puid updatedAt }} }} '


    def query_projects(self,after,first):
        return f'projects(after: "{after}", first: {first}) {{ totalCount nodes {{ createdAt updatedAt id name puid  }} pageInfo {{ endCursor hasNextPage hasPreviousPage startCursor }} }}'
    
    def query_groups(self,after,first):
        return f'groups(after: "{after}", first: {first}) {{ totalCount nodes {{ createdAt updatedAt id name puid }} pageInfo {{ endCursor hasNextPage hasPreviousPage startCursor }} }}'
    
    def query_attachment(self,id):
        return f'node(id: "{id}") {{ id ... on Attachment {{ attachmentUrl byteSize createdAt filename id metadata puid updatedAt }} }} '

    def mutation_update_sample_metadata(self,puid,metadata):
        return f'updateSampleMetadata(input: {{ metadata: {{{metadata}}}, samplePuid: "{puid}"}}) {{ clientMutationId errors status }}'

    def mutation_create_sample(self,name,desc,puid):
        return f'createSample(input: {{ name: "{name}" description: "{desc}", projectPuid: "{puid}" }}) {{ clientMutationId errors {{ message path }} sample {{ id name puid }} }}'

    def mutation_create_direct_upload(self,byteSize,checksum,contentType,filename):
        return f' createDirectUpload( input: {{ byteSize: {byteSize}, checksum: "{checksum}", contentType: "{contentType}", filename: "{filename}" }} ) {{ clientMutationId directUpload {{ blobId headers signedBlobId url }} }}'

    def mutation_attach_file_to_sample(self,signedBlobIDs,puid):
        return f'attachFilesToSample(input: {{ files: {signedBlobIDs}, samplePuid: "{puid}" }}) {{ clientMutationId errors {{ message path }} status }}'



class query_constructor(query_strings):

    def __init__(self) -> None:
        pass

    def queryNamespaces(self,namespaceType, query_names,cursors,puids=[],first=10):
        results = []
        num_puid = len(puids)
        for idx,qname in enumerate(query_names):
            if num_puid == 0:
                if namespaceType == 'project':
                    r = self.query_projects(after=cursors[idx],first=first)
                else:
                    r = self.query_groups(after=cursors[idx],first=first)                
            else:
                if namespaceType == 'project':
                    r = self.query_project_by_puid(puid=puids[idx],after=cursors[idx],first=first)
                else:
                    r = self.query_group_by_puid(puid=puids[idx],after=cursors[idx],first=first)
            results.append(f'{qname}: { r }')
        nl = "\n" #f-string work around
        results = "\n".join(results)
        return f'query {{{nl} {results}       }}'  


    def queryGroupSamples(self,query_names,cursors,ids=[],first=10):
        results = []
        for idx,qname in enumerate(query_names):
            r = self.query_samples_by_group_id(ids[idx],after=cursors[idx],first=first)
            results.append(f'{qname}: { r }')
        nl = "\n"  
        results = "\n".join(results)
        return f'query {{{nl} {results}       }}'  

    def querySamples(self,id_type,query_names,cursors,sample_ids,project_puids=[],first=10):
        results = []
        #invalid, need both name and project
        if id_type == 'name' and len(project_puids) == 0:
            return
        for idx,qname in enumerate(query_names):
            if id_type == 'sample_puid':
                r = self.query_individual_sample_by_puid(after=cursors[idx],first=first,puid=sample_ids[idx])
            else:
                r = self.query_individual_sample_by_name(after=cursors[idx],first=first,sampleName=sample_ids[idx],puid=project_puids) 
            results.append(f'{qname}: { r }')
        nl = "\n"
        results = "\n".join(results)
        return f'query {{{nl} {results}       }}'  
    
    def queryAttachements(self,query_names,ids):
        results = []
        for idx,qname in enumerate(query_names):
            r = self.query_attachment(id=ids[idx])
            results.append(f'{qname}: { r }')
        nl = "\n"
        results = "\n".join(results)
        return f'query {{{nl} {results}       }}'  

    def createSamples(self,query_names, sample_ids,project_puids):
        results = []
        for idx,qname in enumerate(query_names):
            r = self.mutation_create_sample(name=sample_ids[idx],puid=project_puids[idx],desc='') 
            results.append(f'{qname}: { r }')
        
        nl = "\n"
        results = "\n".join(results)
        return f'mutation {{{nl} {results}       }}'
    
    def updateSamples(self,query_names, sample_ids,metadata):
        results = []
        for idx,qname in enumerate(query_names):
            r = self.mutation_update_sample_metadata(puid=sample_ids[idx],metadata=metadata[idx]) 
            results.append(f'{qname}: { r }')
        nl = "\n"
        results = "\n".join(results)
        return f'mutation {{{nl} {results}       }}'

    def createUploads(self,query_names,byteSizes,checksums,contentTypes,filenames):
        results = []
        for idx,qname in enumerate(query_names):
            r = self.mutation_create_direct_upload(byteSize=byteSizes[idx],checksum=checksums[idx],contentType=contentTypes[idx],filename=filenames[idx])
            results.append(f'{qname}: { r }')
        nl = "\n"
        results = "\n".join(results)
        return f'mutation {{{nl} {results}       }}'

    def createAttachments(self,query_names,signedBlobIDs,puids):
        results = []
        for idx,qname in enumerate(query_names):
            r = self.mutation_attach_file_to_sample(puid=puids[idx],signedBlobIDs=signedBlobIDs[idx])
            results.append(f'{qname}: { r }')
        nl = "\n"
        results = "\n".join(results)
        return f'mutation {{{nl} {results}       }}'


    def render(self,query_string):
        return gql(query_string)

