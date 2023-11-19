import json

def read_config():
    with open("extracts.json", "r") as jsonfile:
        data = json.load(jsonfile)
        jsonfile.close()
        return data