from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.requests import RequestsHTTPTransport
from gql import Client
import time
from dataclasses import dataclass


@dataclass
class conf_connect():
    url: str 
    token: str
    timeout: int = 3600

@dataclass
class gql_response():
    response: dict
    errors: dict
    status: bool


class gql_connection:
    connection_time = ''
    def __init__(self,config) -> None:
        self.config = config
        self.timeout = config.timeout
    
    def connect(self):
        # Select your transport with a defined url endpoint
        self.transport = RequestsHTTPTransport(
            url=self.config.access.url, headers={"Authorization": "Basic {}".format(self.config.access.token)}, verify=True,
            retries=3,
        )

        # Create a GraphQL client using the defined transport
        self.client = Client(transport=self.transport, fetch_schema_from_transport=True, execute_timeout=self.timeout)
        self.connection_time = time.time()
    
    def execute(self,query):
        try:
            return self.client.execute(query)
        except Exception as e:
            return {'errors':e}

    def get_error_info(self,d):
        e = {}
        if not isinstance(d, dict) or len(d) == 0:
            return e
        
        key = list(d.keys())[0]
        if isinstance(d[key],Exception):
            return d
        if 'errors' in d[key]:
            e = d[key]['errors']
        return e

    def run(self,gql_query):
        response = self.execute(gql_query)
        errors = self.get_error_info(response)
        if len(errors) > 0:
            response = {}
        return gql_response(response=response,errors=errors,status=True)


class gql_request(gql_connection):
    
    def __init__(self, config):
        super().__init__(config=config)
    
    def get_pagination_info(self,d):
        for key, value in d.items():
            if key == "pageInfo":
                yield value
                break
            yield key
            if isinstance(value, dict):
                yield from self.get_pagination_info(value)

    def parse_pagenation_info(self,d):
        r = self.get_pagination_info(d)
        p = ''
        for k in r:
            if isinstance(k, dict):
                p = k
                break
        after = None
        if len(p) == 0:
            hasNextPage = False
        elif "hasNextPage" in p:
            hasNextPage = p['hasNextPage']
        return (hasNextPage, after)

    

    def request(self,gql_query):
        self.connect()
        response = self.run(gql_query)
        self.client.close_sync()
        return response


