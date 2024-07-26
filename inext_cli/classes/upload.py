import pycurl
from io import BytesIO
import os

class upload:
    def __init__(self) -> None:
        pass

    def submit(self,file_path, upload_url,headers=[]):
        if file_path is None or not os.path.exists(file_path):
            print("File '{}' cant be uploaded".format(file_path))
            return

        c = pycurl.Curl()
        # Set curl session option
        c.setopt(pycurl.URL, upload_url)
        c.setopt(pycurl.UPLOAD, 1)
        c.setopt(pycurl.READFUNCTION, open(file_path, 'rb').read)
        
        # Set size of file to be uploaded.
        c.setopt(pycurl.INFILESIZE, os.path.getsize(file_path))
        c.setopt(pycurl.HTTPHEADER, headers)
        
        data = BytesIO()
        c.setopt(c.WRITEFUNCTION, data.write)

        # Perform a file transfer.
        c.perform()
        c.close()

        # abs url path of the upload file
        return data.getvalue().decode("UTF-8")