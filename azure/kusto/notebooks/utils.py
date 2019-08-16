"""
Collection of useful functions for working with kusto (AzureDataExplorer)
  queries from jupyter notebooks.
"""
import os
import calendar as cal
from datetime import datetime, timedelta, timezone
from ast import literal_eval
from azure.kusto.data.request import KustoClient, KustoConnectionStringBuilder, ClientRequestProperties
from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.data.helpers import dataframe_from_result_table
import pandas as pd
import collections

_client_cache = {}
def get_client(cluster, database):
    """
    get cached, authenticated client for given cluster
    """
    global _client_cache
    c = _client_cache.get(cluster)
    if c is None:
        c = KustoClient(KustoConnectionStringBuilder.with_aad_device_authentication(cluster))
        c.execute(database, 'print "a" | take 0')
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

def to_kusto_timespan(d):
    return 'totimespan("' + str(d) + '")'

def to_datetime(timestamp):
    s = timestamp[:23] + 'Z' # only allow 5 decimals of precision
    for f in ("%Y-%m-%d %H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%S.%fZ"):
        try:
            return datetime.strptime(s, f)
        except:
            pass

def get_time(timestamp, d):
    return int((cal.timegm(to_datetime(timestamp).timetuple()) + (d * 60)) * 1000)

def pandas_df_to_markdown_table(df, index=False):
    fmt = ['---' for i in range(len(df.columns) + (1 if index else 0))]
    df_fmt = pd.DataFrame([fmt],
                          index=(df.index if index else None),
                          columns=df.columns)
    df_formatted = pd.concat([df_fmt, df])
    return df_formatted.to_csv(sep="|", index=index)

def row_as_markdown_table(df):
    headers = ['Property', 'Value']
    rc = '|' + '|'.join(headers) + '|\n'
    rc += '|' + '|'.join(len(headers) * ['---']) + '|\n'
    r = df.loc[0]
    for c in df.columns:
        v = r[c]
        if isinstance(v, str):
            pass
        elif isinstance(v, collections.Sequence):
            v = '<br/>'.join(v)
        else:
            v = str(v)
        rc += '|' + c + '|' + v + '|\n'
    return rc

def quote(s):
    return '"{}"'.format(s)

class Report(object):
    def __init__(self):
        self._content = ''

    def write(self, *args):
        self._content += ''.join([str(a) for a in args]) + '\n'

    @property
    def content(self):
        return self._content