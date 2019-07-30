"""
Collection of useful functions for working with kusto (AzureDataExplorer) 
  queries from jupyter notebooks.
"""
import os
from ast import literal_eval
from azure.kusto.data.request import KustoClient, KustoConnectionStringBuilder, ClientRequestProperties
from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.data.helpers import dataframe_from_result_table

_client_cache = {}
def get_client(cluster):
    """
    get cached, authenticated client for given cluster
    """
    global _client_cache
    c = _client_cache.get(cluster)
    if c is None:
        c = KustoClient(KustoConnectionStringBuilder.with_aad_device_authentication(cluster))
        c.execute('VSO', 'print "a" | take 0')
        _client_cache[cluster] = c
    return c

def execute(client, database, query_name, contents):
    # NOTE: pending PR: https://github.com/Azure/azure-kusto-python/pull/152
    # recover from serialization if necessary
    # if isinstance(client, str): 
    #     client = KustoClient(client)
    #     _client_cache[client.kusto_cluster] = client

    try:
        return client.execute(database, contents)

    except KustoServiceError as error:
        print('Error:', error)
        print('Is semantic error:', error.is_semantic_error())
        
        if error.has_partial_results():
            print('Has partial results.')
            print('Result size:', len(error.get_partial_results()))
        raise error

def execute_file(client, database, path, params, transform = False):
    """
    load csl file and replace tokens using the supplied params dictionary"""
    # load query
    assert os.path.isfile(path), path + ' is not a file'
    with open(path, 'rt') as f:
        contents = f.read()
    assert contents, 'file was empty?'

    # replace parameters
    for k,v in params.items():
        if transform:
            if isinstance(v, str):
                v = '"' + v + '"'        
        contents = contents.replace('{' + k + '}', v)

    # return result
    return execute(client, database, path, contents)

def to_dataframe(result):
    return dataframe_from_result_table(result)

def to_dataframe_from_future(promise):
    return to_dataframe(promise.result().primary_results[0])

def print_result_stats(result):
    info_index = result.tables_names.index('QueryCompletionInformation')
    query_stats = result.tables[info_index]
    stat_column_index = next((x.ordinal for x in query_stats.columns if x.column_name == 'Payload'), None)
    stat_string = query_stats.rows[-1][stat_column_index] # take the last row
    stats = literal_eval(stat_string)
    print('clock time:', stats['ExecutionTime'], 'seconds')
    print('  cpu time:', stats['resource_usage']['cpu']['total cpu'])

def to_kusto_datetime(dt):
    """format date strings to kusto literals"""
    for t in ('now', 'ago'):
        if dt.startswith(t):
            return dt
    return 'datetime(' + str(dt) + ')'
