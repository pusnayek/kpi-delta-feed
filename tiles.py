import json
import requests
import dbconnect
import sys

CONTENT_TYPE_FORM_ENCODED = "application/x-www-form-urlencoded"
CONTENT_TYPE_APPLICATION_JSON = "application/json"
GRANT_TYPE_SAML2_BEARER = "urn:ietf:params:oauth:grant-type:saml2-bearer"

def read_config():
    with open("sfconfig.json", "r") as jsonfile:
        data = json.load(jsonfile)
        jsonfile.close()
        return data


def get_saml_assertion():
    try:
        headers = {'Content-Type': CONTENT_TYPE_FORM_ENCODED}
        data = {
            "client_id"    : CONFIG["CLIENTID"],
            "user_id"      : CONFIG["USERID"],
            "token_url"    : CONFIG["TOKEN_URL"],
            "private_key"  : CONFIG["PRIVATE_KEY"]
        }

        response = requests.post(CONFIG["IDP_URL"], data=data, headers=headers)
        print("Response getting assertion code is {status_code}".format(status_code = response.status_code))
        return response.status_code, response.text
    except Exception as ex:
        raise Exception("ERROR: {message}".format(message=ex.args))        

def get_bearer_token():
    try:
        status_code, assertion = get_saml_assertion()
        headers = {'Content-Type': CONTENT_TYPE_FORM_ENCODED}
        data = {
            "company_id"   : CONFIG["COMPANY_ID"],
            "client_id"    : CONFIG["CLIENTID"],
            "grant_type"   : GRANT_TYPE_SAML2_BEARER,
            "user_id"      : CONFIG["USERID"],        
            "assertion"    : assertion
        }
        response = requests.post(CONFIG["TOKEN_URL"], data=data, headers=headers)
        return response.json()["access_token"]
    except Exception as ex:
        raise Exception("ERROR: {message}".format(message=ex.args))        

def payload(row):
    return {
        "userId": row["ACTING_USERID"],
        "ethnicity": row["CC_ETHNICITY"],
        "__metadata": {
            "uri": "User('{user}')".format(user=row["ACTING_USERID"])
        }
    }

def get_user_clock_values():
    df = dbconnect.read_clock_values()
    data = []
    for index, row in df.iterrows():
        data.append(payload(row))
    # break into chunks
    for i in range(0, len(data), 1000):
        yield data[i:i + 1000]

def update_tile_values():
    try:
        token = get_bearer_token()
        bearer_token = "Bearer {token}".format(token=token)
        headers = {
            'Authorization': bearer_token,
            'Content-Type': CONTENT_TYPE_APPLICATION_JSON
        }
        sys.stdout.write("\nINFO - Bearer token fetched {token}\n".format(token = token))

        # upsert url
        url = "https://api2preview.sapsf.eu/odata/v2/upsert"

        # payload
        # for data in get_user_clock_values():
        #     asyncio.run(post_clock_values(data, headers))

        replicationDone = True
        for data in get_user_clock_values():
            response = requests.post(url, data=json.dumps(data), headers=headers)
            replicationDone = replicationDone and (response.status_code >= 200 and response.status_code < 300)
            sys.stdout.write("\nINFO - SF API call for tiles value update successful\n")
            # print("Response code calling API is {status_code}".format(status_code = response.status_code))
            sys.stdout.write(response.text)

        sys.stdout.write("\nINFO - Replication check {replicationDone}\n".format(replicationDone = replicationDone))
        if(replicationDone):
            dbconnect.drop_clock_deltas()
            print("\nINFO - Clock deltas dropped\n")

    except Exception as ex:
        raise Exception("ERROR: {message}".format(message=ex.args))        

CONFIG = read_config()