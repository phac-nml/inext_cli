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


@dataclass
class file_type_regex():
    """
    Used to control access to the specific file pattern matching available to the user
    """
    assembly: str = ".fasta(.gz)?|.fna(.gz)|.fa(.gz)?|$"
    fasta: str = ".fasta(.gz)?|.fna(.gz)|.fa(.gz)?|.ffn(.gz)?|.faa(.gz)?|$"
    fastq: str = ".fastq(.gz)?|.fq(.gz)$"
    json: str = ".json(.gz)?$"
    genbank: str = ".gbk(.gz)?|.gbf(.gz)|.genbank(.gz)?|$"
    csv: str = ".csv(.gz)$"
    tsv: str = ".tsv(.gz)$"
    text: str = ".txt(.gz)|.text(.gz)$"
    json: str = ".json(.gz)|.js(.gz)$"
    profile: str = ".locidex.report.profile.mlst.subtyping.json.gz$"
    all: str = "."
