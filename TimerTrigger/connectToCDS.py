# -----------------
# connectToCDS.py

# This file contains the function of connecting to Microsoft Dataverse.
# It should be imported and used in the __init__.py file.

import msal
import logging
import requests


def getConfig():
    config = {
    "authority": "https://login.microsoftonline.com/2ec83d1c-69d7-4d7c-a4f8-959cde9d1b46", # login + Azure-ben AAD -> App Registrations -> kiválasztani a megfelelőt -> Directory (tenant) ID
    "client_id": "7fcfca9b-3905-4cf5-b0b2-5c3c74191f45", # Dynamics -> Settings -> Biztonság -> Alkalmazás felhasználók -> Alkalmazást azonosító URI
    "scope": ["https://dallmayrdev.api.crm4.dynamics.com/.default"],
    "secret": "3QT7Q~FN.KRaRFKydxRNwpTZB.rm1j1nQepcz", # Azure -> AAD -> App registrations -> kiválasztani a megfelelőt -> Certificates & Secrets (csak egyszer lehet látni az újonnan létrehozott secretet)
    "endpoint": "https://dallmayrdev.api.crm4.dynamics.com/api/data/v9.2/"
}

    return config

def connect_to_cds(): # a config adatok alapján a bearer tokenen keresztül létrehoz egy access token-t a végén, amivel lehet kapcsolódni a CDS-hez
    # Connect to collectitdev environment

    # Optional logging
    # logging.basicConfig(level=logging.DEBUG)

    config = getConfig()

    # Create a preferably long-lived app instance which maintains a token cache.
    app = msal.ConfidentialClientApplication(
        config["client_id"], authority=config["authority"],
        client_credential=config["secret"],
        # token_cache=...  # Default cache is in memory only.
                        # You can learn how to use SerializableTokenCache from
                        # https://msal-python.rtfd.io/en/latest/#msal.SerializableTokenCache
        )

    # The pattern to acquire a token looks like this.
    result = None

    # Firstly, looks up a token from cache
    # Since we are looking for token for the current app, NOT for an end user,
    # notice we give account parameter as None.
    result = app.acquire_token_silent(config["scope"], account=None)

    if not result:
        print("No suitable token exists in cache. Let's get a new one from AAD.")
        result = app.acquire_token_for_client(scopes=config["scope"])

    if "access_token" in result:
        # Calling graph using the access token
        print('Successfuly connected!')
        return result["access_token"]
    else:
        print("Access token is not available!")
        print(result.get("error"))
        print(result.get("error_description"))
        print(result.get("correlation_id"))  # You may need this when reporting a bug
