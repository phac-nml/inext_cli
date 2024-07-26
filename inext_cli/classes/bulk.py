from copy import deepcopy
from gql import gql, Client
import pandas as pd
from gql.transport.aiohttp import AIOHTTPTransport
import json



NAMESPACE_NODES = ['description', 'fullName', 'fullPath','id','name','path','puid']
SAMPLE_NODES = ['description', 'id','name','metadata','puid']
PAGEINFO_FIELDS = ['endCursor','hasNextPage','hasPreviousPage','startCursor']
namespace_nodes = "\n".join(NAMESPACE_NODES)
pageinfo_fields = "\n".join(PAGEINFO_FIELDS)
sample_nodes = "\n".join(SAMPLE_NODES)

query_projects_str = """
query Projects($first: Int, $after: String) {
    projects(first: $first, after: $after) {
        totalCount
        nodes { """ + namespace_nodes + """
            
        }
        pageInfo { """ + pageinfo_fields + """
            
        }
    }
}

"""


QUERY_PROJECTS = gql(query_projects_str)

class bulk_create_sample:
    create_record_list = [
        'alias',
        ':',
        'createSample(input: {' 
        'projectPuid:','',
        'name:', '',
        'description:','""',
        '})',
        '{sample {name puid}}'
    ]
    def __init__(self) -> None:
        pass
    def create(self,sample_ids,project_ids):
        mutations = []
        for idx,sample_id in enumerate(sample_ids):
            l = deepcopy(self.create_record_list)
            l[0] = f'idx_{idx}'
            l[3] = f'"{project_ids[idx]}"'
            l[5] = f'"{sample_id}"'
            mutations.append(" ".join(l))
        mut_st = 'mutation {' + "\n".join(mutations) + ' }'
        print(mut_st)
        return gql(mut_st)


class bulk_update_metadata:
    update_sample = [
        'alias',
        ':',
        'updateSampleMetadata(input: {', 
        'samplePuid:' '',
        ',' ,
        'metadata:', '' ,
        '}) { clientMutationId errors status } }'
    ]
    
    def __init__(self) -> None:
        pass

    def create(self,sample_ids,metadata):
        mutations = []
        for idx,sample_id in enumerate(sample_ids):
            l = deepcopy(self.update_sample)
            l[0] = f'idx_{idx}'
            l[3] = f'"{metadata[sample_id]}"'
            l[5] = f'"{sample_id}"'
            mutations.append(" ".join(l))
        mut_st = 'mutation {' + "\n".join(mutations) + ' }'
        print(mut_st)
        return gql(mut_st)
    
class bulk_project_sample_query:
    project_samples = [
        'alias',
        ':',
        'project(', 
        'puid:', '' ,
        ') {description fullName fullPath id name path puid samples { totalCount nodes { description id metadata name puid } } } '
    ]
    
    def __init__(self) -> None:
        pass
    
    def create(self,project_ids):
        query = []
        for idx,id in enumerate(project_ids):
            l = deepcopy(self.project_samples)
            l[0] = f'idx_{idx}'
            l[4] = f'"{id}"'
            query.append(" ".join(l))
        mut_st = 'query {' + "\n".join(query) + ' }'
        print(mut_st)
        sys.exit()
        return gql(mut_st)
    
    def 




def create_samples():

    o = bulk_create_sample()
    df = pd.read_csv('/home/jarobert/tmp.txt',header=0,sep="\t")
    sample_ids = df['sample_name'].values.tolist()
    projects = df['irida_next_project_puid'].values.tolist()
    subset_sample_ids = []
    subset_projects = []
    batch_size = 100
    url = 'https://gsp-test.nml-lmn.phac-aspc.gc.ca/api/graphql'
    AUTH_TOKEN = 'amFyb2JlcnRAY3Njc2NpZW5jZS5jYTpxcnBzUUZXZlV6V2ZLM3hfM1F5Uw=='
    responses = {}
    for idx,sample_id in enumerate(sample_ids):
        subset_sample_ids.append(sample_id)
        subset_projects.append(projects[idx])
        if len(subset_projects) == batch_size:
            # Select your transport with a defined url endpoint
            transport = AIOHTTPTransport(
            url=url, headers={"Authorization": "Basic {}".format(AUTH_TOKEN)}
            )

            # Create a GraphQL client using the defined transport
            client = Client(transport=transport, fetch_schema_from_transport=True)
            responses[idx] = client.execute(o.create(subset_sample_ids,subset_projects))
            out = f'/home/jarobert/{idx}.txt'
            with open(out, 'w') as fh:
                fh.write(json.dumps(responses[idx]))

            subset_sample_ids = []
            subset_projects = []

    transport = AIOHTTPTransport(
    url=url, headers={"Authorization": "Basic {}".format(AUTH_TOKEN)}
    )

    # Create a GraphQL client using the defined transport
    client = Client(transport=transport, fetch_schema_from_transport=True)
    responses[idx] = client.execute(o.create(subset_sample_ids,subset_projects), variable_values={})
    out = f'/home/jarobert/{idx}.txt'
    with open(out, 'w') as fh:
        fh.write(json.dumps(responses[idx]))

    subset_sample_ids = []
    subset_projects = []


def bulk_pull(project_ids,batch_size = 100):
    
    o = bulk_create_sample()
    df = pd.read_csv('/home/jarobert/tmp.txt',header=0,sep="\t")
    columns = df.columns.values.to_list()
    sample_ids = df['irida_next_sample_puid'].values.tolist()
    metadata_columns = []
    for col in columns:
        if col in ['irida_next_project_puid','irida_next_sample_puid','sample_name']:
            continue
        metadata_columns.append(col)

    batch_size = 100
    url = 'https://gsp-test.nml-lmn.phac-aspc.gc.ca/api/graphql'
    AUTH_TOKEN = 'amFyb2JlcnRAY3Njc2NpZW5jZS5jYTpxcnBzUUZXZlV6V2ZLM3hfM1F5Uw=='
    responses = {}
    data = {}
    tracker = 0
    count = 0
    for idx,row in df.iterrows():
        sample_id = row['irida_next_sample_puid']
        if count == batch_size:
            count = 0,
            tracker+=1
        data[tracker] = {
            sample_id: {}
        }
        for col in metadata_columns:
            data[tracker][sample_id][col] = row[col] 



    return



o = bulk_project_sample_query()
o.create(['INXT_PRJ_AYHPCMU7VK'])