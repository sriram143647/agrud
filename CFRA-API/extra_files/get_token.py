import requests
session = requests.Session()

token_URL = "https://auth.cfraresearch.com/oauth2/token"
client_ID = "3l7nhm06sin88ru1gjs06nvnhv"
clientSecret = "1s2uvqtvp8fo9scimkarviohdb35fgr5iqmugsh8e6hvpejdj199"
def get_access_token():
    response = requests.post(
        token_URL,
        data={"grant_type": "client_credentials"},
        auth=(client_ID, clientSecret),
    )
    token = 'Bearer ' + response.json()["access_token"] 
    print('access token accquired')
    print(token)


if __name__ == '__main__':
    get_access_token()