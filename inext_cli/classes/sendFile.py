from inext_cli.classes.query import query_constructor
from inext_cli.classes.connect import gql_request
from inext_cli.classes.upload import upload
import os
import hashlib



class sendFile:
    def __init__(self,conf_connect_obj,payload) -> None:
        self.config = conf_connect_obj
        self.payload = payload
        self.query_builder = query_constructor()
        pass


    def request(self,query):
        obj = gql_request(self.config)
        return obj.request(query)

    def parseReserveResponse(self,response):
        blobs = []
        urls = []
        for query_name in response.response:
            blobs.append(response.response[query_name]['directUpload']['signedBlobId'])
            urls.append( response.response[query_name]['directUpload']['url'] )

        return { 'signedBlobIDs':blobs,'urls':urls}

    def parseAttachResponse(self,response):
        pass


    def get_checksum(self, file_name):
        # Open,close, read file and calculate MD5 on its contents 
        with open(file_name, 'rb') as file_to_check:
            # read contents of the file
            data = file_to_check.read()    
            # pipe contents of the file through
            return hashlib.md5(data).hexdigest()
        return ''


    def run(self):
        byteSizes = []
        checksums = []
        contentType = 'application/json'
        contentTypes = []
        filenames = []
        file_paths = []
        samplePuids = []
        for idx in self.payload:         
            files = self.payload[idx]
            for f in files:
                file_paths.append(f)
                samplePuids.append(idx)
                contentTypes.append(contentType)
                filenames.append(os.path.basename(f))
                checksums.append(self.get_checksum(f))
                byteSizes.append(os.stat(f).st_size)
        qnames = []
        for i in range(0,len(samplePuids)):
            qnames.append(f'idx_{i}')

        #Reserve file locations
        query = self.query_builder.createUploads(query_names=qnames, byteSizes=byteSizes, checksums=checksums, contentTypes=contentTypes, filenames=filenames)
        print(query)
        query = self.query_builder.render(query)
        response = self.request(query)
        print(response)
        uploadInfo = self.parseReserveResponse(response)
        #upload files
        for idx,fpath in enumerate(filenames):
            url = uploadInfo['urls'][idx]
            upload_response = upload().submit(fpath, url, ['x-ms-blob-type: BlockBlob', 'x-ms-date: ${DATE_NOW}'])
            print(upload_response)


        #attach files
        data = {}
        for idx,puid in enumerate(samplePuids):
            if not puid in data:
                data[puid] = []
            data[puid].append('"{}"'.format(uploadInfo['signedBlobIDs'][idx]))
        for puid in data:
            data[puid] = f'[{",".join(data[puid])}]'
        query = self.query_builder.createAttachments(query_names=list(data.keys()), puids=list(data.keys()),signedBlobIDs=list(data.values()))
        query = self.query_builder.render(query)
        response = self.request(query)







        
        
