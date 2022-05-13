# -----------------
# recordOperations.py

# This file contains the functions of the basic Microsoft Dataverse Flow actions.
# It should be imported and used in the __init__.py file.

import requests
import json
import logging


# ----------------- Get a row by ID -----------------
def getRecord(guid, entity_name, config):

    crmres = requests.get(config["endpoint"] + entity_name + '(' + guid + ')', headers = config['requestheader'])

    if crmres.status_code >= 400:
        print("There is no record with the given GUID!")
        return -1
    else:
        return json.loads(crmres.content)


# ----------------- Update a row -----------------
# changeObject should be a dictionary (key:value pairs) with keys refering to the field to be changed and value containing the new value of the field.

def updateRecord(changeObject, guid, entity_name, config):

    crmres = requests.patch(config['endpoint'] + entity_name + '('+ guid +')', headers=config['requestheader'], data=json.dumps(changeObject))

    if crmres.status_code >= 400:
        print("The record update could not be performed!")
        print(json.loads(crmres.content))
    else:
        print("The record was updated successfully!")
        return crmres.status_code


# ----------------- List rows -----------------
# 'entity_name' should refer to the entity of which we would like to receive a list.
# 'filt' parameter should be a string character with a format of 'accountid eq 12324-134arq-1233rsf'.
# example:
#    filt = {
#        'filter1': {
#            'field': '_customerid_value',
#            'operator': 'eq',
#            'value': customer_guid
#        },
#        'filter2': {
#            'join_operator':'and',
#            'field': '_customerid_value',
#            'operator': 'eq',
#            'value': customer_guid
#        }
#    }
# To see the list of available filter commands please consult with the following post: https://diyd365.com/2019/11/20/every-power-automate-ms-flow-filter-query-you-ever-wanted-to-know-as-a-functional-consultant/
# 'fields' parameter should be a string representing a list of field names separated by comma e.g. 'name,statecode,duedate'
# Note: Always use at least 1 field! In this case, there's no need to use comma.
def queryRecords(filt, fields, entity_name, config):

    filt = ' '.join([f for fi in filt.keys() for f in filt[fi].values()])
    print("Query filter: {}".format(filt))

    crmres = requests.get(config['endpoint'] + entity_name + '?$select=' + fields + '&$filter=' + filt, headers=config['requestheader'])

    if len(json.loads(crmres.content)['value']) > 0:
        print("Successful query! The response: {}".format(json.loads(crmres.content)['value']))
        return json.loads(crmres.content)['value']
    else:
        print("There is no record for the given filter!")
        return -1


# ----------------- Add a new row -----------------
def createRecord(data, entity_name, config):

    crmres = requests.post(url = config['endpoint'] + entity_name, data = json.dumps(data), headers = config['requestheader'])

    return crmres.status_code