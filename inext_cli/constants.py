from dataclasses import dataclass, field, asdict
from typing import List, Dict

FILE_EXTENSIONS = {
    'tsv': ['txt','tsv','text'],
    'csv': ['csv'],
    'excel': ['xls','xlsx']}

    
@dataclass
class gql_query_parameters:
    qtype: str
    qname: str
    alias: str  = ''
    puid: str = ''
    id: str  = ''
    sampleName: str  = ''
    after: str  = ''
    first:int = 1

@dataclass
class file_extensions:
    extensions: dict = field(default_factory= lambda: FILE_EXTENSIONS )
    file_types: List[str] | None = None

    def __post_init__(self):
        if self.file_types is None:
            self.file_types = list(self.extensions.keys())



