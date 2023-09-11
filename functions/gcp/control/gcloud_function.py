from urllib import request

import google.auth.transport.requests
import google.oauth2.id_token

# check if we need to setup role: https://cloud.google.com/functions/docs/securing/authenticating#generating_tokens_programmatically
class CloudFunction:
    def __init__(self, region: str, project_id: str) -> None:
        # top-cascade-392319
        self.root_endpoint = f"https://{region}-{project_id}.cloudfunctions.net/"
        self.root_audience = f"https://{region}-{project_id}.cloudfunctions.net/"
    def invoke(self, FunctionName: str, Payload):
        # For Cloud Functions, `endpoint` and `audience` should be equal
        endpoint = self.root_endpoint + FunctionName
        audience = self.root_audience + FunctionName
        req = request.Request(endpoint, data=Payload)

        auth_req = google.auth.transport.requests.Request()
        id_token = google.oauth2.id_token.fetch_id_token(auth_req, audience)

        req.add_header("Authorization", f"Bearer {id_token}")
        
        response = request.urlopen(req)

        return response.read()