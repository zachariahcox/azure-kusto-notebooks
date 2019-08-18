from azure.kusto.notebooks.NotebookKustoClient import NotebookKustoClient
from azure.kusto.notebooks import utils as akn

from adal import AuthenticationContext, AdalError
from adal.constants import TokenResponseFields, OAuth2DeviceCodeResponseParameters


def test_token():

    ac = AuthenticationContext("https://login.microsoftonline.com/{0}".format('common'))
    kusto_cluster = 'https://vso.kusto.windows.net'
    client_id = "db662dc1-0cfe-4e1c-a843-19a68e65be58"
    
    # fetch parameters
    user_code_info = ac.acquire_user_code(kusto_cluster, client_id)
    
    # open window for user
    user_code = user_code_info[OAuth2DeviceCodeResponseParameters.USER_CODE]
    url = user_code_info[OAuth2DeviceCodeResponseParameters.VERIFICATION_URL]
    
    # wait for user to input user_code
    token = ac.acquire_token_with_device_code(kusto_cluster, user_code_info, client_id)

    c = NotebookKustoClient(kusto_cluster, token)
    resp = c.execute_query('VSO', 'print a|take 0')
    assert resp is not None

if __name__ == "__main__":
    test_token()