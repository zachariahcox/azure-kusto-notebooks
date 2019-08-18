from azure.kusto.data.request import KustoClient, KustoResponseDataSetV1, KustoResponseDataSetV2, KustoServiceError
from azure.kusto.data._version import VERSION

from adal import AuthenticationContext, AdalError
from adal.constants import TokenResponseFields, OAuth2DeviceCodeResponseParameters

from datetime import timedelta
import requests
from requests.adapters import HTTPAdapter
import dateutil
import json
from copy import copy
import uuid


class NotebookKustoClient(object):
    _query_default_timeout = timedelta(minutes=4, seconds=30)

    # The maximum amount of connections to be able to operate in parallel
    _max_pool_size = 100

    def __init__(self, cluster, adal_context):
        kusto_cluster = cluster

        # Create a session object for connection pooling
        self._session = requests.Session()
        self._session.mount("http://", HTTPAdapter(pool_maxsize=self._max_pool_size))
        self._session.mount("https://", HTTPAdapter(pool_maxsize=self._max_pool_size))
        self._query_endpoint = "{0}/v2/rest/query".format(kusto_cluster)
        self._request_headers = {
            "Accept": "application/json",
            "Accept-Encoding": "gzip,deflate",
            "x-ms-client-version": "Kusto.Python.Client:" + VERSION,
        }
        
        # auth
        assert isinstance(adal_context, AuthenticationContext)
        self._adal_context = adal_context or AuthenticationContext("https://login.microsoftonline.com/common")
        self._client_id = "db662dc1-0cfe-4e1c-a843-19a68e65be58" # lifted from azure.kusto.data

    @property
    def client_id():
        return self._client_id
    
    def execute_query(self, database, query, properties=None):
        """Executes a query.
        :param str database: Database against query will be executed.
        :param str query: Query to be executed.
        :return: Kusto response data set.
        :rtype: azure.kusto.data._response.KustoResponseDataSet
        """
        return self._execute(
            self._query_endpoint, database, query, None, KustoClient._query_default_timeout, properties
        )

    def _execute(self, endpoint, database, query, payload, timeout, properties=None):
        """Executes given query against this client"""
        request_headers = copy(self._request_headers)
        json_payload = None
        if not payload:
            json_payload = {"db": database, "csl": query}
            if properties:
                json_payload["properties"] = properties.to_json()

            request_headers["Content-Type"] = "application/json; charset=utf-8"
            request_headers["x-ms-client-request-id"] = "KPC.execute;" + str(uuid.uuid4())
        else:
            request_headers["x-ms-client-request-id"] = "KPC.execute_streaming_ingest;" + str(uuid.uuid4())
            request_headers["Content-Encoding"] = "gzip"
            if properties:
                request_headers.update(json.loads(properties.to_json())["Options"])
        
        # original
        # if self._auth_provider:
        #     request_headers["Authorization"] = self._auth_provider.acquire_authorization_header()
        # timeout = self._get_timeout(properties, timeout)

        # new!
        request_headers["Authorization"] = self.acquire_authorization_header()

        # post request
        response = self._session.post(
            endpoint, 
            headers=request_headers, 
            data=payload, 
            json=json_payload, 
            timeout=timeout.seconds
        )

        if response.status_code == 200:
            if endpoint.endswith("v2/rest/query"):
                return KustoResponseDataSetV2(response.json())
            return KustoResponseDataSetV1(response.json())

        raise KustoServiceError([response.json()], response)


    def acquire_authorization_header(self):
        """Acquire tokens from AAD."""
        try:
            return self._acquire_authorization_header()
        except AdalError as error:
            kwargs = {"client_id": self._client_id}
            kwargs["resource"] = self._kusto_cluster
            kwargs["authority"] = self._adal_context.authority.url
            raise KustoAuthenticationError(self._authentication_method.value, error, **kwargs)


    def _acquire_authorization_header(self):
        # load token from cache
        token = self._adal_context.acquire_token(self._kusto_cluster, None, self._client_id)
        if token is not None:
            expiration_date = dateutil.parser.parse(token[TokenResponseFields.EXPIRES_ON])
            if expiration_date > datetime.now() + timedelta(minutes=1):
                return _get_header_from_dict(token)
            if TokenResponseFields.REFRESH_TOKEN in token:
                token = self._adal_context.acquire_token_with_refresh_token(
                    token[TokenResponseFields.REFRESH_TOKEN], self._client_id, self._kusto_cluster
                )
                if token is not None:
                    return _get_header_from_dict(token)

        # create a new token from scratch
        code = self._adal_context.acquire_user_code(self._kusto_cluster, self._client_id)
        print(code[OAuth2DeviceCodeResponseParameters.MESSAGE])
        webbrowser.open(code[OAuth2DeviceCodeResponseParameters.VERIFICATION_URL])
        token = self._adal_context.acquire_token_with_device_code(self._kusto_cluster, code, self._client_id)

        return _get_header_from_dict(token)


    def _get_header_from_dict(token):
        return _get_header(token[TokenResponseFields.TOKEN_TYPE], token[TokenResponseFields.ACCESS_TOKEN])


    def _get_header(token_type, access_token):
        return "{0} {1}".format(token_type, access_token)