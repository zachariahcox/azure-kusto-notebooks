from azure.kusto.notebooks import utils as akn




def test_get_client():
    c = akn.get_client('https://vso.kusto.windows.net')
    akn.execute(c, 'VSO', 'print "a" | take 0')

if __name__ == "__main__":
    test_get_client()