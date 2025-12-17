import requests


class SpotifyApiClient:
    def __init__(self, client_id:str, client_secret:str):
        self.base_url="https://api.spotify.com/v1"

        if client_id is None:
            raise Exception("API key cannot be set to None.")
        self.client_id = client_id

        if client_secret is None:
            raise Exception("API secret key cannot be set to None.")
        self.client_secret = client_secret

        #Request Access Token
        #The Client Credentials flow is used in server-to-server authentication. 
        # Since this flow does not include authorization, only endpoints that do not access user information can be accessed.
        auth_url = "https://accounts.spotify.com/api/token"
        headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }
        body = {"grant_type": "client_credentials",
                "client_id":client_id,
                "client_secret":client_secret}
        
        #Request Access Token
        auth_response = requests.post(auth_url, headers=headers, data=body)
        auth_response.raise_for_status()
        self.access_token=auth_response.json()['access_token']
        self.token_type=auth_response.json()['token_type']


