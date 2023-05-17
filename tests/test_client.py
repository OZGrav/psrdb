from cli.graphql_client import GraphQLClient


def test_graphql_client():
    url = "http://127.0.0.1:8000/graphql"
    verbose = False
    client = GraphQLClient(url, verbose)
    try:
        client.connect()
    except:
        print("Caught connection exception")
