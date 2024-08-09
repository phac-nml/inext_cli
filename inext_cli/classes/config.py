from dataclasses import dataclass, field
from inext_cli.constants import file_extensions

@dataclass
class config:
    id_type: str
    input_path:str
    access : dataclass
    outputs: dataclass
    n_records: int = 1
    n_cursors: int = 1
    ignore_empty: bool = True
    n_threads : int = 1
    metadata_cols : list = field(default_factory=list)
    file_cols : list = field(default_factory=list)
    skip_rows : int = 0
    create: bool = False
    skip_meta: bool = False
    project_code : str = ''
    project_col: str = ''
    id_col: str = ''
    file_regex: dataclass = field(default_factory= lambda: file_extensions())
    wait_time: int = 1
    timeout: int = 60
    batch_size: int = 100
    download: bool = False
    download_regex: str = "."
    download_workers: int = 1

    

@dataclass
class analysisDir:
    baseDir: str
    dataDir: str 
    logFile: str
    errorFile: str
    resultsFile: str
    projectIndexFile: str = ''
    groupIndexFile: str = ''
    sampleIndexFile:str = ''
    attachIndexFile: str = ''


