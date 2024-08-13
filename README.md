[![PyPI](https://img.shields.io/badge/Install%20with-PyPI-blue)](https://pypi.org/project/inext_cli/#description)
[![Bioconda](https://img.shields.io/badge/Install%20with-bioconda-green)](https://anaconda.org/bioconda/inext_cli)
[![Conda](https://img.shields.io/conda/dn/bioconda/locidex?color=green)](https://anaconda.org/bioconda/inext_cli)
[![License: Apache-2.0](https://img.shields.io/github/license/phac-nml/inext_cli)](https://www.apache.org/licenses/LICENSE-2.0)

# IRIDA Next command line interface tool kit

- [Install](#install)
    + [Compatibility](#compatibility)
- [Getting Started](#getting-started)
  * [Usage](#usage)
  * [Configuration and Settings](#configuration-and-settings)
    + [Push](#push)
    + [Pull](#pull)
- [Troubleshooting and FAQs](#troubleshooting-and-faqs)
- [Contact](#contact)


<small><i><a href='http://ecotrust-canada.github.io/markdown-toc/'>Table of contents generated with markdown-toc</a></i></small>



# Install

Install the latest released version from conda:

        conda create -c bioconda -c conda-forge -n inext inext_cli

Install using pip:

        pip install  inext_cli

Install the latest master branch version directly from Github:

        pip install git+https://github.com/phac-nml/inext_cli.git


# Getting Started

## Usage

		inext_cli <command> [options] <required arguments>

### Commands

IRIDA Next CLI uses the following commands:

1. **push** - Create samples, upload files, attach files to samples, update sample metadata based on a sample sheet
2. **pull** - Pull sample metadata, files bases on IRIDA persistent unique identifiers for groups, projects and samples, in addition to project and sample name based on a sample sheet
2. **download** - Download files associated with IRIDA Next records based on the attachment file produces by pull

## Configuration and Settings



### Push


EXAMPLE: Create a set of samples with fasta files:

		inext_cli push 

EXAMPLE: Create a set of samples with paired-end fastq files:

		inext_cli push 

#### Input

Sample Sheet can be in one of the following formats (tab delimited: txt, tsv, comma delimeted: csv, Excel: xls, xlsx). 
Configuration parameters can be provided in json format.
All files for uploading to IRIDA Next should be compressed using gzip. Push will not upload files which are not compressed.

#### Output

```
{out folder name} 
├── err.log    
├── run.log
└── samples.tsv
```


### Pull

#### Input

EXAMPLE: Pull all user project, group and sample metadata but skip files :

		inext_cli pull

EXAMPLE: Pull metadata and fasta files for a set of samples:

		inext_cli pull --download --file_type fasta

EXAMPLE: Pull metadata and fastq files for a set of samples:

		inext_cli pull --download --file_type fastq 

#### Output
```
{out folder name} 
└──data
    ├──{Batch #}
        ├── {IRIDA Next Persistent Identifier Sample 1}
                ├── {IRIDA Next Persistent Identifier Attachment 1}
                    ├── sample_1_1.fastq.gz
                    └──sample_1_2.fastq.gz
                ├── ...
                └── n
        ├── ....
        ├── {IRIDA Next Persistent Identifier Sample N}
        └── manifest.txt
├── err.log    
├── run.log
├── attachIndex.tsv
├── projectIndex.tsv
├── groupIndex.tsv
└── samplesIndex.tsv
```
samples.tsv
```

### Download

#### Input

EXAMPLE: Pull all user project, group and sample metadata but skip files :

		inext_cli download

EXAMPLE: Pull files for a set of samples:

		inext_cli download

EXAMPLE: Pull files for a set of samples:

		inext_cli download

#### Output
```
{out folder name} 
├──{Batch #}
    ├── {IRIDA Next Persistent Identifier Sample 1}
            ├── {IRIDA Next Persistent Identifier Attachment 1}
                ├── sample_1_1.fastq.gz
                └──sample_1_2.fastq.gz
            ├── ...
            └── n
    ├── ....
    ├── {IRIDA Next Persistent Identifier Sample N}
    └── manifest.txt
├── err.log    
└── run.log
```



### config.json

This [JSON](https://www.json.org/json-en.html) file can be used in place of specifying each parameter on the command line. The behaviour is that command line arguments will *NOT* override parameters set in the configuration file. So only set the fields
that you want to be persistent in the configuration file.

```
        {
            "url": "",
            "token": "",
            "id_col": "irida_next_project_code",
            "id_type": "project",
            "file_type": "all",
            "skip_rows": 0,
            "download": true,
            "workers": 10,
            "n_cursors": 100,
            "batch_size": 100,
            "file_cols": "",
            "metadata_cols":"",
            "create": false,
            "skip_meta": false,
            "project_code": "",
            "project_col": "",
            "download_workers":10
        }
```

# Troubleshooting and FAQs

## FAQ

**Coming soon**

# Contact

For any questions, issues or comments please make a Github issue or reach out to [**James Robertson**](james.robertson@phac-aspc.gc.ca).

# Legal and Compliance Information

Copyright Government of Canada 2024

Written by: National Microbiology Laboratory, Public Health Agency of Canada

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this work except in compliance with the License. You may obtain a copy of the License at:

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

