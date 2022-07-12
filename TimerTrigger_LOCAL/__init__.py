import datetime
from math import prod
from xmlrpc.client import Boolean
from dateutil.relativedelta import relativedelta
from dateutil import tz
import logging
import json
import connectToCDS as ctc
import recordOperations as ro
import requests
import http.client

def get_token(auth_url, client_id, scope, client_secret, grant_type = 'client_credentials'):
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    url =auth_url
    data = { "client_id": client_id,
            "scope": scope,
            "client_secret": client_secret,
            "grant_type": grant_type
        }
    r = requests.post(url=url, data=data, headers=headers)
    return r.json(), r.status_code

auth_url = 'https://login.microsoftonline.com/2ec83d1c-69d7-4d7c-a4f8-959cde9d1b46/oauth2/v2.0/token'
client_id = 'd6b218d6-f97d-4917-88f9-3cf7c33aa4bc'
scope = 'https://api.businesscentral.dynamics.com/.default'
client_secret = "VIK7Q~p3T20CCPQ~f8-rLRGcIZNZz93c-QlcN"
print("-------------------CONNECT TO BUSINESS CENTRAL---------------------")
auth_url = 'https://login.microsoftonline.com/2ec83d1c-69d7-4d7c-a4f8-959cde9d1b46/oauth2/v2.0/token'
client_id = 'd6b218d6-f97d-4917-88f9-3cf7c33aa4bc'
scope = 'https://api.businesscentral.dynamics.com/.default'
client_secret = "VIK7Q~p3T20CCPQ~f8-rLRGcIZNZz93c-QlcN"
token = get_token(auth_url, client_id, scope, client_secret)
access_token = token[0]['access_token']
header_token = {"Authorization": "Bearer {}".format(access_token)}
#bcHead = requests.get(url="https://api.businesscentral.dynamics.com/v2.0/2ec83d1c-69d7-4d7c-a4f8-959cde9d1b46/Production/ODataV4/Company('Dallmayr')/VL_PostedSalesInvoices", headers=header_token).json()["value"]
#bcLine = requests.get(url="https://api.businesscentral.dynamics.com/v2.0/2ec83d1c-69d7-4d7c-a4f8-959cde9d1b46/Production/ODataV4/Company('Dallmayr')/VL_PostedSalesInvoiceLines",headers=header_token).json()["value"]
bcHead=[]
bcLine=[]

def main():
    commandName='monthly'
    method='normal'
    testData={ "Parancs":"test", "Futas":"monthly", "Szamlafejek":{ "values":[ ] }, "Szamlasorok":{ "values":[ ] }, "Vevocsoportok":[ ], "Partnerek":[ { "name":"Teszt Partner_000009", "vl_limitfigyeles_szintje_partner": False, "_vl_vevocsoport_partner_value":"None", "_parentaccountid_value":"None", "accountid":"67616d24-f35d-ec11-8f8f-111111111119", "vl_szamlazasi_periodus":"None", "vl_szamlazasi_periodus_kezdete":"None", "vl_akt_szaml_per_vege":"None" } ], "Szerzodesek":[ { "_vl_szamlazasi_partner_szerzodes_value":"67616d24-f35d-ec11-8f8f-111111111119", "vl_szerzodes_statusza":100000002, "vl_jovahagyas_statusza": True, "vl_szerzodesszam":"TEST_001008", "vl_limit_figyeles_szintje_szerz": True, "_vl_ugyfel_value":"67616d24-f35d-ec11-8f8f-111111111119", "vl_szamlazasi_periodus_kezdete_szerzodes":"2022-06-01T00:00:00Z", "vl_szamlazasi_periodus_szerzodes":100000001, "vl_szerzodes_kategoria":100000000, "vl_szerzodes_lejarata":"2023-09-30T00:00:00Z", "vl_szerzodesekid":"324c7596-0972-ec11-8943-111111111009" } ], "Szerzodessorok":[ { "vl_koztes_limit_berleti_dija_huf":"None", "vl_koztes_limit_berleti_dija_eur":"None", "vl_teljes_berleti_dij_huf":"None", "vl_teljes_berleti_dij_eur":"None", "vl_fix_berleti_dij":6000, "vl_name":"TEST_001008-SOR-00001", "_vl_szerzodes_value":"324c7596-0972-ec11-8943-111111111009", "vl_limittel_erintett_szerzodessor": False, "vl_limitertek_alapja":"None", "vl_szamlazas_kezdete":"2022-06-10T00:00:00Z", "vl_szamlazas_vege":"None", "_vl_kapcsolodo_pos_value":"995ea8d3-1a5f-ec11-8f8f-000d3a2324d1", "vl_szamlazo":100000001, "vl_limit_erteke":"None", "vl_limit_koztes_ertek":"None", "vl_berl_dij_min_nem_telj_eseten_fix":"None", "vl_berl_dij_min_telj_eseten_huf":"None", "vl_berl_dij_max_feletti_telj_eseten":"None", "vl_szerzodessoraiid":"ceace059-63e3-ec11-bb3d-000000000015" } ], "Limittermekek":[ ] }
    global bcHead
    global bcLine
    if method=='test':
        bcHead=testData['Szamlafejek']["values"]
        bcLine=testData['Szamlasorok']["values"]
    #else:
    #    bcHead = requests.get(url="https://api.businesscentral.dynamics.com/v2.0/2ec83d1c-69d7-4d7c-a4f8-959cde9d1b46/Production/ODataV4/Company('Dallmayr')/VL_PostedSalesInvoices", headers=header_token).json()["value"]
    #    bcLine = requests.get(url="https://api.businesscentral.dynamics.com/v2.0/2ec83d1c-69d7-4d7c-a4f8-959cde9d1b46/Production/ODataV4/Company('Dallmayr')/VL_PostedSalesInvoiceLines",headers=header_token).json()["value"]
    response=[""]
    # Connect to Dataverse
    print("\n ---------------------------------- CONNECT TO DATAVERSE ---------------------------------- ")
    print("Creating config file!")
    config = ctc.getConfig() # defines config data
    config["requestheader"] = {
        'Authorization': 'Bearer ' + ctc.connect_to_cds(), # 'Bearer' + access_token
        'Content-Type': 'application/json',
        'OData-MaxVersion': '4.0',
        'OData-Version': '4.0'
    }

    print("\n ---------------------------------- CHECK LIMIT HANDLING LEVEL ---------------------------------- ")

    filt = {
            'filter1': {
                'field': 'vl_limit_megadasi_helye_szintje',
                'operator': 'eq',
                'value': 'true'
            }
    }
    print("Listing CustomerGroups with limit handle level")
    listedCustomerGroups=[]
    if method != 'test':
        listedCustomerGroups = ro.queryRecords(filt, 'vl_limit_megadasi_helye_szintje,vl_name,vl_vevocsoport_szamlazasi_periodus,vl_vevocs_szamlazasi_periodus_kezdete', 'vl_vevocsoports', config)
    else:
        for customerGroup in testData['Vevocsoportok']:
            if customerGroup['vl_limit_megadasi_helye_szintje'] is True:
                listedCustomerGroups.append(customerGroup)
    if listedCustomerGroups != -1:
        limit_on_customergroup(response,listedCustomerGroups,commandName,method,testData,config)
    
    filt = {
            'filter1': {
                'field': 'vl_limitfigyeles_szintje_partner',
                'operator': 'eq',
                'value': 'true'
            }
    }
    print("Listing Customers with limit handle level")
    listedCustomers=[]
    if method != 'test':
        listedCustomers = ro.queryRecords(filt, 'name,vl_szamlazasi_periodus,vl_szamlazasi_periodus_kezdete,vl_limitfigyeles_szintje_partner,_vl_vevocsoport_partner_value,_parentaccountid_value', 'accounts', config)
    else:
        for customer in testData['Partnerek']:
            if customer['vl_limitfigyeles_szintje_partner'] is True:
                listedCustomers.append(customer)
    #Szerződések szűréséhez, ha kikerülne a partner a listából mert van fölötte lévő szint
    listedCustomersForContracts = listedCustomers[:]
    if listedCustomers != -1:
        if listedCustomerGroups != -1:
            for customer in list(listedCustomers):
                if customer['_parentaccountid_value'] is None or customer['_parentaccountid_value']=='None':
                    pass
                else:
                    if method != 'test':
                        parentCustomer=ro.getRecord(customer['_parentaccountid_value'],'accounts',config)
                    else:
                        for parent in listedCustomers:
                            if customer['_parentaccountid_value'] == parent['accountid']:
                                parentCustomer=parent
                    if parentCustomer['vl_limitfigyeles_szintje_partner'] is True:
                        listedCustomers.remove(customer)
            for customer in listedCustomers:
                for group in listedCustomerGroups:
                    if customer['_vl_vevocsoport_partner_value'] == group['vl_vevocsoportid']:
                        listedCustomers.remove(customer)
        limit_on_customer(response,listedCustomers,commandName,method,testData,100000001,None,None,[0,0,0,0,0,0,0,0],None,None,config)
    
    filt = {
            'filter1': {
                'field': 'vl_limit_figyeles_szintje_szerz',
                'operator': 'eq',
                'value': 'true'
            }
    }
    print("Listing Contracts with limit handle level")
    listedContracts=[]
    if method != 'test':
        listedContracts = ro.queryRecords(filt, '_vl_szamlazasi_partner_szerzodes_value,vl_szerzodes_statusza,vl_jovahagyas_statusza,vl_szerzodesszam,vl_limit_figyeles_szintje_szerz,_vl_ugyfel_value,vl_szamlazasi_periodus_kezdete_szerzodes,vl_szamlazasi_periodus_szerzodes,vl_szerzodes_kategoria,vl_szerzodes_lejarata', 'vl_szerzodeseks', config)
    else:
        for contract in testData['Szerzodesek']:
            if contract['vl_limit_figyeles_szintje_szerz'] is True:
                listedContracts.append(contract)
    if listedContracts != -1:
        if listedCustomersForContracts != -1:
            for contract in list(listedContracts):
                #Teszthez
                #if contract['vl_szerzodesszam']=='ADSZERZ-001065':
                #    limit_on_contract(response,[contract],commandName,method,testData,100000000,None,None,[0,0,0,0,0,0,0,0],None,None,config)
                if  contract['vl_jovahagyas_statusza']!=True:
                    listedContracts.remove(contract)
                else:
                    for customer in listedCustomersForContracts:
                        if customer['accountid'] == contract['_vl_ugyfel_value']:
                            listedContracts.remove(contract)
        limit_on_contract(response,listedContracts,commandName,method,testData,100000000,None,None,[0,0,0,0,0,0,0,0],None,None,config)
    
    print("Válasz: "+str(response[0]))

def calculate_correction(periodStart,periodEnd,periodDays,linePeriodStart,linePeriodEnd):
    if (datetime.datetime.today()-periodStart-relativedelta(days=periodDays)).days<=0:
        if periodStart != linePeriodStart: 
            substrNum=(linePeriodStart-periodStart).days
            correction=1-substrNum/periodDays
            if correction < 0:
                return 0
            else: return correction
        else:
            return 1
    elif (datetime.datetime.today()-periodStart-relativedelta(days=periodDays)).days>0:
        #ha határozatlan idejű szerz és a sornak sincs számlázás vége
        if linePeriodEnd == "" or linePeriodEnd is None or periodEnd=="" or periodEnd is None:
            return 1
        elif periodEnd != linePeriodEnd: 
            substrNum=(periodEnd-linePeriodEnd).days
            if (periodEnd-periodStart).days%periodDays!=0:
                correction=1-substrNum/(periodDays-((periodEnd-periodStart).days%periodDays))
            if correction < 0:
                return 0 
            else: return correction
        else:
            return 1

def drop_pos(periodStart,periodEnd,linePeriodStart,linePeriodEnd,periodDays,posVatDate):
    #aktív számlázási periódus e
    #ha határozatlan idejű szerz és a sornak sincs számlázás vége
    if periodEnd is None or periodEnd=="":
        if datetime.datetime.strptime(posVatDate,'%Y-%m-%d') >= datetime.datetime.today()-relativedelta(days=periodDays):
            return True
        else: return False
    elif linePeriodEnd is None or linePeriodEnd=="":
        if datetime.datetime.strptime(posVatDate,'%Y-%m-%d') >= datetime.datetime.today()-relativedelta(days=periodDays):
            return True
        else: return False
    elif periodStart <= posVatDate and posVatDate <= periodEnd:
        if linePeriodStart <= posVatDate and posVatDate <= linePeriodEnd and posVatDate >= datetime.datetime.today()-relativedelta(days=periodDays): 
            return True
        else: return False
    else: return False

def AD_calculate(response,contractLine,contract,periodStart,periodDays,bcHead,bcLine,pos,contractProductsKg,contractProductsHuf,contractProductsUnit):
    if contractLine['vl_limittel_erintett_szerzodessor'] is True:
        #ha nincs megadva számlázás vége akkor a szerződés vége lesz az ??mivan ha ez sincs?
        if contractLine['vl_szamlazas_vege'] is None or contractLine['vl_szamlazas_vege']=='None': 
            if contract['vl_szerzodes_lejarata'] is None or contract['vl_szerzodes_lejarata']=='None':
                szamlazasVege=""
            else:
                szamlazasVege = datetime.datetime.strptime(str(contract['vl_szerzodes_lejarata']).split('T')[0],'%Y-%m-%d')
        else:
            szamlazasVege = datetime.datetime.strptime(contractLine['vl_szamlazas_vege'].split('T')[0],'%Y-%m-%d')
        if contractLine['vl_szamlazas_kezdete'] is None:
            billingStart=periodStart
        else: billingStart=contractLine['vl_szamlazas_kezdete']
        if contract['vl_szerzodes_lejarata'] is None or contract['vl_szerzodes_lejarata']=='None':
            contract['vl_szerzodes_lejarata']="2060-01-01T00:00:00Z"
        correction=calculate_correction(datetime.datetime.strptime(periodStart.split('T')[0],'%Y-%m-%d'),datetime.datetime.strptime(contract['vl_szerzodes_lejarata'].split('T')[0],'%Y-%m-%d'),periodDays,datetime.datetime.strptime(billingStart.split('T')[0],'%Y-%m-%d'),szamlazasVege)
        print("Korrekció: "+str(correction))
        response[0]=response[0]+"\nKorrekció: "+str(correction)
        postedSalesInvoiceLines=[]
        postedSalesInvoiceHeads=[]
        #Eladási sorok kilistázása a megadott pos-hez
        for line in bcLine:
            ok=False
            if line['Shortcut_Dimension_2_Code']==pos['vl_pos_id']:
                #if contractLine['vl_limitertek_alapja'] == 100000002:
                #    ok=True
                #else:
                if contractProductsHuf:
                    for product in contractProductsHuf:
                        if line['No'] == product['productnumber']:
                            ok=True
                elif contractProductsKg:
                    for product in contractProductsKg:
                        if line['No'] == product['productnumber']:
                            ok=True
                elif contractProductsUnit:
                    for product in contractProductsUnit:
                        if line['No'] == product['productnumber']:
                            ok=True
                if ok:
                    for head in bcHead:
                        if line['Document_No']==head['No']:
                            if drop_pos(periodStart,contract['vl_szerzodes_lejarata'],contractLine['vl_szamlazas_kezdete'],contractLine['vl_szamlazas_vege'],periodDays,head['Shipment_Date']):
                                postedSalesInvoiceLines.append(line)
                                postedSalesInvoiceHeads.append(head)
        #Eladott mennyiségek összegzése
        #kg
        if postedSalesInvoiceLines:
            token = get_token(auth_url, client_id, scope, client_secret)
            access_token = token[0]['access_token']
            header_token = {"Authorization": "Bearer {}".format(access_token)}
            products = requests.get(url="https://api.businesscentral.dynamics.com/v2.0/2ec83d1c-69d7-4d7c-a4f8-959cde9d1b46/Production/ODataV4/Company('Dallmayr')/Cikk_karton_Excel", headers=header_token).json()["value"]
            if contractLine['vl_limitertek_alapja'] == 100000000:
                    for line in postedSalesInvoiceLines:
                            for product in products:
                                if product['No']==line['No']:
                                    return float(line['Quantity'])*float(product['Net_Weight'])
            #huf
            elif contractLine['vl_limitertek_alapja'] == 100000001:
                    for line in postedSalesInvoiceLines:
                        return float(line['Amount'])
            elif contractLine['vl_limitertek_alapja'] == 100000002:
                    for line in postedSalesInvoiceLines:
                        if line['Unit_of_Measure']=="DB":
                            return float(line['Quantity'])
            else: return 0
        else: return 0            

def limit_on_customergroup(response,listedCustomerGroups,commandName,method,testData,config):
    for group in listedCustomerGroups:
        #[eladottKg,eladottOsszeg,eladottMennyiseg,limitKg,limitOsszeg,limitMennyiseg,vegosszeg]
        dailyDataVcs=[0,0,0,0,0,0,0,0]
        currPeriodStart=None
        currPeriodEnd=None
        period=group['vl_vevocsoport_szamlazasi_periodus']
        periodStart=group['vl_vevocs_szamlazasi_periodus_kezdete']
        filt = {
            'filter1': {
                'field': '_vl_vevocsoport_partner_value',
                'operator': 'eq',
                'value': group['vl_vevocsoportid']
            }
        }
        listedCustomers=[]
        if method!='test':
            listedCustomers = ro.queryRecords(filt, 'name,vl_szamlazasi_periodus,vl_szamlazasi_periodus_kezdete,vl_limitfigyeles_szintje_partner,_vl_vevocsoport_partner_value,_parentaccountid_value', 'accounts', config)
        else:
            for customer in testData['Partnerek']:
                if customer['_vl_vevocsoport_partner_value'] == group['vl_vevocsoportid']:
                    listedCustomers.append(customer)
        if listedCustomers != -1:
                print("-------------------------LIMIT ON CUSTOMERGROUP--------------------------")
                response[0]=response[0]+"\n-------------------------LIMIT ON CUSTOMERGROUP--------------------------"
                for customer in list(listedCustomers):
                    if customer['_parentaccountid_value'] is None or customer['_parentaccountid_value']=='None':
                        pass
                    else:
                        if method != 'test':
                            parentCustomer=ro.getRecord(customer['_parentaccountid_value'],'accounts',config)
                        else:
                            for parent in listedCustomers:
                                if customer['_parentaccountid_value'] == parent['accountid']:
                                    parentCustomer=parent
                        if parentCustomer['_vl_vevocsoport_partner_value'] ==group['vl_vevocsoportid']:
                            listedCustomers.remove(customer)
                limit_on_customer(response,listedCustomers,commandName,method,testData,100000002,currPeriodStart,currPeriodEnd,dailyDataVcs,period,periodStart,config)
                if commandName=="daily" and not all(v == 0 for v in dailyDataVcs):
                    print("Vevőcsoport neve: "+group['vl_name'])
                    print("Eladott Kg: "+str(dailyDataVcs[0]))
                    print("Eladott Összeg: "+str(dailyDataVcs[1]))
                    print("Eladott Mennyiség: "+str(dailyDataVcs[2]))
                    print("Limit Kg: "+str(dailyDataVcs[3]))
                    print("Limit Összeg: "+str(dailyDataVcs[4]))
                    print("Limit Mennyiség: "+str(dailyDataVcs[5]))
                    print("Számlázandó összeg (Forint): "+str(dailyDataVcs[6]))
                    print("Számlázandó összeg (Euro): "+str(dailyDataVcs[7]))
                    response[0]=response[0]+"\nVevőcsoport neve: "+group['vl_name']
                    response[0]=response[0]+"\nEladott Kg: "+str(dailyDataVcs[0])
                    response[0]=response[0]+"\nEladott Összeg: "+str(dailyDataVcs[1])
                    response[0]=response[0]+"\nEladott Mennyiség: "+str(dailyDataVcs[2])
                    response[0]=response[0]+"\nLimit Kg: "+str(dailyDataVcs[3])
                    response[0]=response[0]+"\nLimit Összeg: "+str(dailyDataVcs[4])
                    response[0]=response[0]+"\nLimit Mennyiség: "+str(dailyDataVcs[5])
            response[0]=response[0]+"\nSzámlázandó összeg (Forint): "+str(dailyDataVcs[6])
                    response[0]=response[0]+"\nSzámlázandó összeg (Euro): "+str(dailyDataVcs[7])
                    data = {
                        "vl_eladott_kg_vcs":dailyDataVcs[0],
                        "vl_eladott_osszeg_vcs":dailyDataVcs[1],
                        "vl_eladott_mennyiseg_vcs":dailyDataVcs[2],
                        "vl_limit_kg_vcs":dailyDataVcs[3],
                        "vl_limit_osszeg_vcs":dailyDataVcs[4],
                        "vl_limit_mennyiseg_vcs":dailyDataVcs[5],
                        "vl_vegosszeg_huf_vcs":str(dailyDataVcs[6]),
                        "vl_vegosszeg_huf_vcs":str(dailyDataVcs[7]),
                        "vl_hatralevo_kg_vcs":dailyDataVcs[3]-dailyDataVcs[0],
                        "vl_hatralevo_osszeg_vcs":dailyDataVcs[4]-dailyDataVcs[1],
                        "vl_hatralevo_mennyiseg_vcs":dailyDataVcs[5]-dailyDataVcs[6],
                        "vl_akt_sszaml_per_kezd_vcs":currPeriodStart,
                        "vl_akt_sszaml_per_vege_vcs":currPeriodEnd
                    }
                    if method !='test':
                        ro.updateRecord(data,group['vl_vevocsoportid'],'vl_vevocsoports',config)

def limit_on_customer(response,listedCustomers,commandName,method,testData,limitLevel,currPeriodStart,currPeriodEnd,dailyDataVcs,period,periodStart,config):
    for customer in listedCustomers:
        if limitLevel==100000001:
            period=customer['vl_szamlazasi_periodus']
            periodStart=customer['vl_szamlazasi_periodus_kezdete']
        #[eladottKg,eladottOsszeg,eladottMennyiseg,limitKg,limitOsszeg,limitMennyiseg,vegosszeg]
        dailyData=[0,0,0,0,0,0,0,0]
        filt = {
            'filter1': {
                'field': '_vl_ugyfel_value',
                'operator': 'eq',
                'value': customer['accountid']
            }
        }
        listedContracts=[]
        if method != 'test':
            listedContracts = ro.queryRecords(filt, '_vl_szamlazasi_partner_szerzodes_value,vl_szerzodes_statusza,vl_jovahagyas_statusza,vl_szerzodesszam,vl_limit_figyeles_szintje_szerz,_vl_ugyfel_value,vl_szamlazasi_periodus_kezdete_szerzodes,vl_szamlazasi_periodus_szerzodes,vl_szerzodes_kategoria,vl_szerzodes_lejarata', 'vl_szerzodeseks', config)
        else:
            for contract in testData['Szerzodesek']:
                if contract['_vl_ugyfel_value'] == customer['accountid']:
                    listedContracts.append(contract)
        if listedContracts != -1:
            print("-------------------------LIMIT ON CUSTOMER--------------------------")
            response[0]=response[0]+"\n-------------------------LIMIT ON CUSTOMER--------------------------"
            for contract in list(listedContracts):
                if  contract['vl_jovahagyas_statusza']!=True:
                    listedContracts.remove(contract)
            if listedContracts != -1:
                limit_on_contract(response,listedContracts,commandName,method,testData,limitLevel,currPeriodStart,currPeriodEnd,dailyData,period,periodStart,config)
                dailyDataVcs[0]+=dailyData[0]
                dailyDataVcs[1]+=dailyData[1]
                dailyDataVcs[2]+=dailyData[2]
                dailyDataVcs[3]+=dailyData[3]
                dailyDataVcs[4]+=dailyData[4]
                dailyDataVcs[5]+=dailyData[5]
                dailyDataVcs[6]+=dailyData[6]
                dailyDataVcs[7]+=dailyData[7]
                if commandName=="daily" and limitLevel==100000001 and not all(v == 0 for v in dailyData):
                    print("Partner neve: "+customer['name'])
                    print("Eladott Kg: "+str(dailyData[0]))
                    print("Eladott Összeg: "+str(dailyData[1]))
                    print("Eladott Mennyiség: "+str(dailyData[2]))
                    print("Limit Kg: "+str(dailyData[3]))
                    print("Limit Összeg: "+str(dailyData[4]))
                    print("Limit Mennyiség: "+str(dailyData[5]))
                    print("Számlázandó összeg (Forint): "+str(dailyData[6]))
                    print("Számlázandó összeg (Euro): "+str(dailyData[7]))
                    response[0]=response[0]+"\nPartner neve: "+customer['name']
                    response[0]=response[0]+"\nEladott Kg: "+str(dailyData[0])
                    response[0]=response[0]+"\nEladott Összeg: "+str(dailyData[1])
                    response[0]=response[0]+"\nEladott Mennyiség: "+str(dailyData[2])
                    response[0]=response[0]+"\nLimit Kg: "+str(dailyData[3])
                    response[0]=response[0]+"\nLimit Összeg: "+str(dailyData[4])
                    response[0]=response[0]+"\nLimit Mennyiség: "+str(dailyData[5])
                    response[0]=response[0]+"\nSzámlázandó összeg (Forint): "+str(dailyData[6])
                    response[0]=response[0]+"\nSzámlázandó összeg (Euro): "+str(dailyData[7])
                    data = {
                        "vl_eladott_kg_partner":dailyData[0],
                        "vl_eladott_osszeg_partner":dailyData[1],
                        "vl_eladott_mennyiseg_partner":dailyData[2],
                        "vl_limit_kg_partner":dailyData[3],
                        "vl_limit_osszeg_partner":dailyData[4],
                        "vl_limit_mennyiseg_partner":dailyData[5],
                        "vl_vegosszeg_huf":str(dailyData[6]),
                        "vl_vegosszeg_eur_partner":str(dailyData[7]),
                        "vl_hatralevo_kg_partner":dailyData[3]-dailyData[0],
                        "vl_hatralevo_osszeg_partner":dailyData[4]-dailyData[1],
                        "vl_hatralevo_mennyiseg_partner":dailyData[5]-dailyData[6],
                        "vl_akt_szaml_per_kezd_partner":currPeriodStart,
                        "vl_akt_szaml_per_vege":currPeriodEnd
                    }
                    if method != 'test':
                        ro.updateRecord(data,customer['accountid'],'accounts',config)
        else: print("Nincsenek szerződések a " + customer['name']+" nevű ügyfélhez")

def limit_on_contract(response,listedContracts,commandName,method,testData,limitLevel,currPeriodStart,currPeriodEnd,dailyData,period,periodStart,config):
    print("-------------------------LIMIT ON CONTRACT--------------------------")
    response[0]=response[0]+"\n-------------------------LIMIT ON CONTRACT--------------------------"
    soldKgPartner=0
    soldHufPartner=0
    soldUnitPartner=0
    limitKgPartner=0
    limitHufPartner=0
    limitUnitPartner=0
    billPartner=0
    billPartner2=0
    runningFee=0
    for contract in listedContracts:
        print('!!!! Szerződés: '+str(contract['vl_szerzodesszam'])+' !!!!')
        response[0]=response[0]+"\n!!!! Szerződés: "+str(contract['vl_szerzodesszam'])+" !!!!"
        #csekkoljuk melyik szinten van a periódus
        if limitLevel == 100000000:
            period=contract['vl_szamlazasi_periodus_szerzodes']
            periodStart=contract['vl_szamlazasi_periodus_kezdete_szerzodes']
        # Számlázási periódus első napja (Origo - szerződés legelső napja)
        if periodStart is None or periodStart=="":
            print("Nincs megadva a periódus kezdete")
            response[0]=response[0]+"\nNincs megadva a periódus kezdete"
            continue
        first_day_of_the_contract_period = datetime.datetime.strptime(
            periodStart, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=tz.gettz('UTC')).astimezone(tz.gettz('Budapest/Europe')).replace(tzinfo=None)

        print("Szamlazasi periodus elso napja: {}".format(first_day_of_the_contract_period))
        
        #teljesítési periodus megadása
        if commandName=="monthly":
            # Elemzett szamlazasi periodus vege
            last_day_of_prev_month = (datetime.datetime.today().replace(day=1)-relativedelta(days=1)).date()

            if period == 100000000:
                deltaMonths = 0
                monthsPeriodToAdd = 1
                # first_day_of_the_period=(last_day_of_prev_month-relativedelta(months=0)).replace(day=1)-relativedelta(days=1)
                # periodDays=(last_day_of_prev_month-first_day_of_the_period).days
                billRowCount=0
            elif period == 100000001:
                deltaMonths = 2
                monthsPeriodToAdd = 3
                #billingStart=datetime.datetime.strptime(contract['vl_szamlazasi_periodus_kezdete_szerzodes'].split('T')[0],'%Y-%m-%d')
                #while (billingStart+relativedelta(months=3))>last_day_of_prev_month:
                #       billingStart=billingStart+relativedelta(months=3)
                # first_day_of_the_period=(last_day_of_prev_month-relativedelta(months=2)).replace(day=1)-relativedelta(days=1)
                # periodDays=(last_day_of_prev_month-first_day_of_the_period).days
                billRowCount=2       
            elif period == 100000002:
                deltaMonths = 5
                monthsPeriodToAdd = 6
                # first_day_of_the_period=(last_day_of_prev_month-relativedelta(months=5)).replace(day=1)-relativedelta(days=1)
                # periodDays=(last_day_of_prev_month-first_day_of_the_period).days
                billRowCount=5 
            elif period == 100000003:
                deltaMonths = 11
                monthsPeriodToAdd = 12
                # first_day_of_the_period=(last_day_of_prev_month-relativedelta(months=11)).replace(day=1)-relativedelta(days=1)
                # periodDays=(last_day_of_prev_month-first_day_of_the_period).days 
                billRowCount=11  
            else:
                print('Nincs megadva periódus a szerződéhez')
                response[0]=response[0]+"\nNincs megadva periódus a szerződéhez"
                continue

            first_day_of_the_period = ((last_day_of_prev_month-relativedelta(months=deltaMonths)).replace(day=1))

            print("Az elemzett periodus kezdesi datuma: {}".format(first_day_of_the_period))
            
            first_day_shifted = first_day_of_the_contract_period.date()
            
            while(first_day_shifted < first_day_of_the_period):
                first_day_shifted = first_day_shifted + relativedelta(months=monthsPeriodToAdd)
                print("+{} honappal: {}".format(monthsPeriodToAdd, first_day_shifted))

            if (first_day_of_the_period == first_day_shifted):
                currPeriodStart=str(first_day_of_the_period)
                currPeriodEnd=str(first_day_shifted + relativedelta(months=monthsPeriodToAdd)-relativedelta(days=1))
                print("A szerzodes szamlazhato!")
                print("Szerzodes +{} honappal: {} -- Szerzodes kezdodatuma ha honap vegen jar le: {}".format(
                    monthsPeriodToAdd, first_day_shifted, first_day_of_the_period))
            else:
                currPeriodStart=str(first_day_shifted)
                currPeriodEnd=str(first_day_shifted + relativedelta(months=monthsPeriodToAdd)-relativedelta(days=1))
                print("A szamlazasi periodus kezdodatuma nem talal!")
                if first_day_of_the_period < first_day_of_the_contract_period.date():
                    first_day_of_the_period = first_day_of_the_contract_period.date()
                    response[0]=response[0]+"\nAktuális periódus kezdete:"+currPeriodStart
                    response[0]=response[0]+"\nAktuális periódus vége:"+currPeriodEnd
                    print("Akt1:"+str(currPeriodStart))
                    print("Akt2:"+str(currPeriodEnd))
                    continue
                print("Szerzodes +{} honappal: {} -- Szerzodes kezdodatuma ha honap vegen jar le: {}".format(
                    monthsPeriodToAdd, first_day_shifted, first_day_of_the_period))

            periodDays = (last_day_of_prev_month - first_day_of_the_period).days + 1


        elif commandName=="daily":
            last_day_of_current_month=((datetime.datetime.today()+relativedelta(months=1)).replace(day=1)-relativedelta(days=1)).date()
            current_day = (datetime.datetime.today()).date()
            if period == 100000000:
                deltaMonths = 0
                monthsPeriodToAdd = 1
                # first_day_of_the_period=(last_day_of_current_month-relativedelta(months=0)).replace(day=1)-relativedelta(days=1)
                # periodDays=(last_day_of_current_month-first_day_of_the_period).days
                billRowCount=0
            elif period == 100000001:
                deltaMonths = 2
                monthsPeriodToAdd = 3
                # first_day_of_the_period=(last_day_of_current_month-relativedelta(months=2)).replace(day=1)-relativedelta(days=1)
                # periodDays=(last_day_of_current_month-first_day_of_the_period).days
                billRowCount=2            
            elif period == 100000002:
                deltaMonths = 5
                monthsPeriodToAdd = 6
                # first_day_of_the_period=(last_day_of_current_month-relativedelta(months=5)).replace(day=1)-relativedelta(days=1)
                # periodDays=(last_day_of_current_month-first_day_of_the_period).days   
                billRowCount=5
            elif period == 100000003:
                deltaMonths = 11
                monthsPeriodToAdd = 12
                # first_day_of_the_period=(last_day_of_current_month-relativedelta(months=11)).replace(day=1)-relativedelta(days=1)
                # periodDays=(last_day_of_current_month-first_day_of_the_period).days
                billRowCount=11 
            else:
                print('Nincs megadva periódus a szerződéhez')
                response[0]=response[0]+"\nNincs megadva periódus a szerződéhez"
                continue

            first_day_of_the_period = ((last_day_of_current_month-relativedelta(months=deltaMonths)).replace(day=1))

            print("Az elemzett periodus kezdesi datuma: {}".format(first_day_of_the_period))
            
            first_day_shifted = first_day_of_the_contract_period.date()
            
            while(first_day_shifted < first_day_of_the_period):
                first_day_shifted = first_day_shifted + relativedelta(months=monthsPeriodToAdd)
                print("+{} honappal: {}".format(monthsPeriodToAdd, first_day_shifted))

            if (first_day_of_the_period == first_day_shifted):
                currPeriodStart=str(first_day_of_the_period)
                currPeriodEnd=str(first_day_shifted + relativedelta(months=monthsPeriodToAdd)-relativedelta(days=1))
                print("A szerzodes szamlazhato!")
                print("Szerzodes +{} honappal: {} -- Szerzodes kezdodatuma ha honap vegen jar le: {}".format(
                    monthsPeriodToAdd, first_day_shifted, first_day_of_the_period))
            else:
                currPeriodStart=str(first_day_shifted - relativedelta(months=monthsPeriodToAdd))
                currPeriodEnd=str(first_day_shifted-relativedelta(days=1))
                print("A szamlazasi periodus kezdodatuma nem talal!")
                if first_day_of_the_period < first_day_of_the_contract_period.date():
                    first_day_of_the_period = first_day_of_the_contract_period.date()
                    currPeriodStart=str(first_day_shifted)
                    currPeriodEnd=str(first_day_shifted + relativedelta(months=monthsPeriodToAdd)-relativedelta(days=1))
                print("Szerzodes +{} honappal: {} -- Szerzodes kezdodatuma ha honap vegen jar le: {}".format(
                    monthsPeriodToAdd, first_day_shifted, first_day_of_the_period))

            periodDays = (current_day - first_day_of_the_period).days + 1

            print("Szerzodes aktualis periodusa: {} - {} --- hossza: {} nap".format(
                first_day_of_the_period, current_day, periodDays))

        print("Akt1:"+str(currPeriodStart))	
        print("Akt2:"+str(currPeriodEnd))
        print('Periódus hossza: '+str(periodDays))
        response[0]=response[0]+"\nPeriódus hossza: "+str(periodDays)
        response[0]=response[0]+"\nAktuális periódus kezdete:"+currPeriodStart
        response[0]=response[0]+"\nAktuális periódus vége:"+currPeriodEnd
        #Szerződés sorainak listázása
        filt = {
            'filter1': {
                'field': '_vl_szerzodes_value',
                'operator': 'eq',
                'value': contract['vl_szerzodesekid']
            }
        }
        listedContractLines=[]
        if method !='test':
            listedContractLines = ro.queryRecords(filt, 'vl_koztes_limit_berleti_dija_huf,vl_koztes_limit_berleti_dija_eur,vl_teljes_berleti_dij_huf,vl_teljes_berleti_dij_eur,vl_fix_berleti_dij,vl_name,_vl_szerzodes_value,vl_limittel_erintett_szerzodessor,vl_limitertek_alapja,vl_szamlazas_kezdete,vl_szamlazas_vege,_vl_kapcsolodo_pos_value,vl_szamlazo,vl_limit_erteke,vl_limit_koztes_ertek,vl_berl_dij_min_nem_telj_eseten_fix,vl_berl_dij_min_telj_eseten_huf,vl_berl_dij_max_feletti_telj_eseten', 'vl_szerzodessorais', config)
        else:
            for contractLine in testData['Szerzodessorok']:
                if contractLine['_vl_szerzodes_value'] == contract['vl_szerzodesekid']:
                    listedContractLines.append(contractLine)
        soldKg=0
        soldHuf=0
        soldUnit=0
        limitKg=0
        limitHuf=0
        limitUnit=0
        halfLimitKg=0
        halfLimitHuf=0
        halfLimitUnit=0
        if listedContractLines !=-1:
            #Szerződés termék sorainak listázása
            filt = {
                'filter1': {
                    'field': 'vl_termek_hierarchia',
                    'operator': 'eq',
                    'value': '100000000'
                }
            }
            allProducts=ro.queryRecords(filt, '_vl_folerendelt_termek_csoport_value,productnumber', 'products', config)
            filt = {
                'filter1': {
                    'field': '_vl_szerzodesek_limit_termek_value',
                    'operator': 'eq',
                    'value': contract['vl_szerzodesekid']
                }
            }
            contractProducts=[]
            if method !='test':
                contractProducts = ro.queryRecords(filt, '_vl_szerzodesek_limit_termek_value,vl_limitvizsgalatialap,_vl_kapcsolodo_termek_value,_vl_vallalt_termek_csoport_value', 'vl_limittermeksorais', config)
            else:
                for product in testData['Limittermekek']:
                    if product['_vl_szerzodesek_limit_termek_value'] == contract['vl_szerzodesekid']:
                        contractProducts.append(product)
            contractProductsKg=[]
            contractProductsHuf=[]
            contractProductsUnit=[]
            if contractProducts == -1:
                print('Nincs megadva a szerződéshez termék')
                response[0]=response[0]+"\nNincs megadva a szerződéshez termék"
                continue
            #ezt így utólag nemtudom jó e.
            for product in contractProducts:
                if product['vl_limitvizsgalatialap']==100000000:
                    if product['_vl_kapcsolodo_termek_value'] is None or product['_vl_kapcsolodo_termek_value']=='None' or product['_vl_kapcsolodo_termek_value']=="":
                        for prod in allProducts:
                            if product['_vl_vallalt_termek_csoport_value']==prod['_vl_folerendelt_termek_csoport_value']:
                                contractProductsKg.append(prod)
                    else:
                        contractProductsKg.append(ro.getRecord(product['_vl_kapcsolodo_termek_value'],'products',config))
                elif product['vl_limitvizsgalatialap']==100000001:
                    if product['_vl_kapcsolodo_termek_value'] is None or product['_vl_kapcsolodo_termek_value']=='None' or product['_vl_kapcsolodo_termek_value']=="":
                        for prod in allProducts:
                            if product['_vl_vallalt_termek_csoport_value']==prod['_vl_folerendelt_termek_csoport_value']:
                                contractProductsHuf.append(prod)                    
                    else:
                        contractProductsHuf.append(ro.getRecord(product['_vl_kapcsolodo_termek_value'],'products',config))
                else: 
                    if product['_vl_kapcsolodo_termek_value'] is None or product['_vl_kapcsolodo_termek_value']=='None' or product['_vl_kapcsolodo_termek_value']=="":
                        for prod in allProducts:
                            if product['_vl_vallalt_termek_csoport_value']==prod['_vl_folerendelt_termek_csoport_value']:
                                contractProductsUnit.append(prod)                    
                    else:
                        contractProductsUnit.append(ro.getRecord(product['_vl_kapcsolodo_termek_value'],'products',config))

            
            #Horeca vagy Vending szerződés
            posDictionary={}
            if contract['vl_szerzodes_kategoria'] == 100000000 or contract['vl_szerzodes_kategoria'] == 100000001:
                for contractLine in listedContractLines:
                    # Számlázó Dallmayr
                    if contractLine['vl_szamlazo'] == 100000001:
                        if contractLine['_vl_kapcsolodo_pos_value'] is None or contractLine['_vl_kapcsolodo_pos_value']=='None':
                            continue
                        pos=ro.getRecord(contractLine['_vl_kapcsolodo_pos_value'],'vl_poses',config)
                        posDictionary[contractLine['vl_name']]=pos['vl_posid']
                        #limites sor
                        if contractLine['vl_limittel_erintett_szerzodessor']==True:
                            print('Szerződés sor: '+str(contractLine['vl_name']))
                            response[0]=response[0]+"\nSzerződés sor: "+str(contractLine['vl_name'])
                            print('POS: '+str(pos['vl_pos_id']))
                            response[0]=response[0]+"\nPOS: "+str(pos['vl_pos_id'])
                            #Kg
                            if contractLine['vl_limitertek_alapja'] == 100000000:
                                soldKg+=AD_calculate(response,contractLine,contract,periodStart,periodDays,bcHead,bcLine,pos,contractProductsKg,contractProductsHuf,contractProductsUnit)
                                limitKg+=float(contractLine['vl_limit_erteke'])
                                #if contractLine['vl_limit_koztes_ertek']:
                                #    halfLimitKg+=float(contractLine['vl_limit_koztes_ertek'])
                            #Huf
                            elif contractLine['vl_limitertek_alapja'] == 100000001:
                                soldHuf+=AD_calculate(response,contractLine,contract,periodStart,periodDays,bcHead,bcLine,pos,contractProductsKg,contractProductsHuf,contractProductsUnit)
                                limitHuf+=float(contractLine['vl_limit_erteke'])
                                #if contractLine['vl_limit_koztes_ertek']:
                                #    halfLimitHuf+=float(contractLine['vl_limit_koztes_ertek'])
                            #Unit
                            elif contractLine['vl_limitertek_alapja'] == 100000002:
                                soldUnit+=AD_calculate(response,contractLine,contract,periodStart,periodDays,bcHead,bcLine,pos,contractProductsKg,contractProductsHuf,contractProductsUnit)
                                limitUnit+=float(contractLine['vl_limit_erteke'])
                                if contractLine['vl_limit_koztes_ertek']:
                                    halfLimitUnit+=float(contractLine['vl_limit_koztes_ertek'])
        '''if halfLimitHuf:
            halfLimitHuf=limitHuf*(halfLimitHuf/100)
        if halfLimitKg:
            halfLimitKg=limitKg*(halfLimitKg/100)
        if halfLimitUnit:
            halfLimitUnit=limitUnit*(halfLimitUnit/100)'''
        if listedContractLines != -1:                    
            print('Eladott kg:'+str(soldKg))
            response[0]=response[0]+"\nEladott kg:"+str(soldKg)
            soldKgPartner+=soldKg
            print('Eladott összeg: ' + str(soldHuf))
            response[0]=response[0]+"\nEladott összeg: " + str(soldHuf)
            soldHufPartner+=soldHuf
            print('Eladott mennyiség: '+str(soldUnit))
            response[0]=response[0]+"\nEladott mennyiség: "+str(soldUnit)
            soldUnitPartner+=soldUnit
            print('Limit kg: '+str(limitKg))
            response[0]=response[0]+"\nLimit kg: "+str(limitKg)
            limitKgPartner+=limitKg
            print('Limit összeg: '+str(limitHuf))
            response[0]=response[0]+"\nLimit összeg: "+str(limitHuf)
            limitHufPartner+=limitHuf
            print('Limit mennyiség: '+str(limitUnit))
            response[0]=response[0]+"\nLimit mennyiség: "+str(limitUnit)
            limitUnitPartner+=limitUnit
            #print('Köztes limit kg: '+str(halfLimitKg))
            #print('Köztes limit összeg: '+str(halfLimitHuf))
            #print('Köztes limit mennyiség: '+str(halfLimitUnit))
            id=datetime.datetime.today().timestamp()
            missing=False
            missingData={}
            missingDataData=[]
            missingDataLine={}
            missingDataLineData=[]
            if limitLevel==1000000002 and commandName=="monthly":
                if contract['vl_szerzodesekid'] is None or contract['vl_szerzodesekid']=='None':
                    missing=True               
                    missingDataData.append(contract['vl_szerzodesekid'])
                if contract['_vl_szamlazasi_partner_szerzodes_value'] is None or contract['_vl_szamlazasi_partner_szerzodes_value']=='None':
                    missing=True
                    missingDataData.append("Számlázási Partner")
                if contract['_vl_ugyfel_value'] is None or contract['_vl_ugyfel_value']=='None':
                    missing=True
                    missingDataData.append("Ügyfél")
                if  method!='test':
                    if ro.getRecord(contract['_vl_ugyfel_value'],'accounts',config)['_vl_vevocsoport_parner_value'] is None:
                        missing=True
                        missingDataData.append("Vevőcsoport")
                if missing:
                    missingData[contract['vl_szerzodesszam']]=missingDataData
                data = {
                    "vl_Szerzodesek@odata.bind": "vl_szerzodeseks({})".format(contract['vl_szerzodesekid']),
                    "vl_Szolgszaml_szamlazasi_partner@odata.bind":"accounts({})".format(contract['_vl_szamlazasi_partner_szerzodes_value']),
                    "vl_Account@odata.bind":"accounts({})".format(contract['_vl_ugyfel_value']),
                    "vl_Vevocsoport@odata.bind":"accounts({})".format(ro.getRecord(contract['_vl_ugyfel_value'],'accounts',config)['_vl_vevocsoport_parner_value']),
                    "vl_eladott_kg":soldKg,
                    "vl_eladott_mennyiseg":soldUnit,
                    "vl_eladott_osszeg":soldHuf,
                    "vl_Limit_kg":limitKg,
                    "vl_limit_mennyiseg":limitUnit,
                    "vl_limit_osszeg":limitHuf,
                    "vl_periodus":periodDays,
                    "vl_limit_szint":limitLevel,
                    "vl_Name":contract['vl_szerzodesszam'],
                    "vl_Szolg_szamla_tipusa":100000000,
                    "vl_vl_Aktualis_szamlazasi_per_kez":currPeriodStart,
                    "vl_Aktualis_szamlazasi_per_veg":currPeriodEnd,
                    #ideiglenes id mező
                    "vl_korrekcio":id
                }
            elif commandName=="monthly":
                if contract['vl_szerzodesekid'] is None or contract['vl_szerzodesekid']=='None':
                    missing=True               
                    missingDataData.append(contract['vl_szerzodesekid'])
                if contract['_vl_szamlazasi_partner_szerzodes_value'] is None or contract['_vl_szamlazasi_partner_szerzodes_value']=='None':
                    missing=True
                    missingDataData.append("Számlázási Partner")
                if contract['_vl_ugyfel_value'] is None or contract['_vl_ugyfel_value']=='None':
                    missing=True
                    missingDataData.append("Ügyfél")
                if missing:
                    missingData[contract['vl_szerzodesszam']]=missingDataData
                data = {
                    "vl_Szerzodesek@odata.bind": "vl_szerzodeseks({})".format(contract['vl_szerzodesekid']),
                    "vl_Szolgszaml_szamlazasi_partner@odata.bind":"accounts({})".format(contract['_vl_szamlazasi_partner_szerzodes_value']),
                    "vl_Account@odata.bind":"accounts({})".format(contract['_vl_ugyfel_value']),
                    "vl_eladott_kg":soldKg,
                    "vl_eladott_mennyiseg":soldUnit,
                    "vl_eladott_osszeg":soldHuf,
                    "vl_Limit_kg":limitKg,
                    "vl_limit_mennyiseg":limitUnit,
                    "vl_limit_osszeg":limitHuf,
                    "vl_periodus":periodDays,
                    "vl_limit_szint":limitLevel,
                    "vl_Name":contract['vl_szerzodesszam'],
                    "vl_Szolg_szamla_tipusa":100000000,
                    "vl_vl_Aktualis_szamlazasi_per_kez":currPeriodStart,
                    "vl_Aktualis_szamlazasi_per_veg":currPeriodEnd,
                    #ideiglenes id mező
                    "vl_korrekcio":id
                }
            if commandName=="monthly" and missing:
                data = {
                    "vl_eladott_kg":soldKg,
                    "vl_eladott_mennyiseg":soldUnit,
                    "vl_eladott_osszeg":soldHuf,
                    "vl_Limit_kg":limitKg,
                    "vl_limit_mennyiseg":limitUnit,
                    "vl_limit_osszeg":limitHuf,
                    "vl_periodus":periodDays,
                    "vl_limit_szint":limitLevel,
                    "vl_Name":contract['vl_szerzodesszam'],
                    "vl_Szolg_szamla_tipusa":100000000,
                    "vl_vl_Aktualis_szamlazasi_per_kez":currPeriodStart,
                    "vl_Aktualis_szamlazasi_per_veg":currPeriodEnd,
                    #ideiglenes id mező
                    "vl_korrekcio":id
                }
                if method != 'test':
                    if ro.createRecord(data,'vl_szolgaltatasszamlas',config) >= 400:
                        print("Nem sikerült a létrehozás!")
                        print("Line814\n{}".format(data))
                    else:
                        filt = {
                            'filter1': {
                                'field': 'vl_limit_szint',
                                'operator': 'eq',
                                'value': str(limitLevel)
                            }
                        }
                        invoices=ro.queryRecords(filt,'vl_korrekcio,vl_name','vl_szolgaltatasszamlas',config)
                        for record in invoices:
                            if int(record['vl_korrekcio'])==int(id) and record['vl_name']==contract['vl_szerzodesszam']:
                                invoice=record
                else:
                    invoice={'vl_szolgaltatasszamlaid':"12"}
            elif commandName=="monthly" and not missing:
                if method != 'test':
                    if ro.createRecord(data,'vl_szolgaltatasszamlas',config) >= 400:
                        print("Nem sikerült a létrehozás!")
                        print("Line832\n{}".format(data))
                    else:
                        filt = {
                            'filter1': {
                                'field': 'vl_limit_szint',
                                'operator': 'eq',
                                'value': str(limitLevel)
                            }
                        }
                        
                        invoices=ro.queryRecords(filt,'vl_korrekcio,vl_name','vl_szolgaltatasszamlas',config)
                        for record in invoices:
                            if int(record['vl_korrekcio'])==int(id) and record['vl_name']==contract['vl_szerzodesszam']:
                                invoice=record
                else:
                    invoice={'vl_szolgaltatasszamlaid':"12"}
            deviza=100000000
            for contractLine in listedContractLines:
                missingDataLineData=[]
                boolean=False
                if (contractLine['vl_teljes_berleti_dij_huf'] is None  or contractLine['vl_teljes_berleti_dij_huf']=='None' and contractLine['vl_koztes_limit_berleti_dija_huf'] is None or contractLine['vl_koztes_limit_berleti_dija_huf']=='None'):
                    if (contractLine['vl_teljes_berleti_dij_eur'] is None or contractLine['vl_teljes_berleti_dij_eur']=='None' and contractLine['vl_koztes_limit_berleti_dija_eur'] is None or contractLine['vl_koztes_limit_berleti_dija_eur']=='None'):
                        if contractLine['vl_fix_berleti_dij'] is None or contractLine['vl_fix_berleti_dij']=='None':
                            boolean=True
                            missingDataLineData.append("Bérleti díjak")
                if not contractLine['vl_name'] in posDictionary:
                    boolean=True
                    missingDataLineData.append("POS")
                if boolean:
                    missing=True
                    missingDataLine[contractLine['vl_name']]=missingDataLineData
            for i in range(billRowCount+1):
                #
                # Teljes limit elerve -- Fix dij/Limit elerve
                #
                if soldKg >= limitKg and soldUnit >= limitUnit and soldHuf >= limitHuf and not missing:
                    for contractLine in listedContractLines:
                        if contractLine['vl_teljes_berleti_dij_huf'] is not None and contractLine['vl_teljes_berleti_dij_huf']!='None':
                            deviza=100000000
                            totalFee=contractLine['vl_teljes_berleti_dij_huf']
                        elif contractLine['vl_teljes_berleti_dij_eur'] is not None and contractLine['vl_teljes_berleti_dij_eur']!='None':
                                deviza=100000001
                                totalFee=contractLine['vl_teljes_berleti_dij_eur']
                        # 
                        # HA nem limites szerzodes -- Fix dijas
                        # 
                        if contractLine['vl_limittel_erintett_szerzodessor'] is False:
                            if commandName=="monthly":
                                data = {
                                    "vl_vl_Szerzodo_Partner@odata.bind":"accounts({})".format(contract['_vl_ugyfel_value']),
                                    "vl_Name":contractLine['vl_name'],
                                    "vl_Szolgaltatas_szamla@odata.bind":"vl_szolgaltatasszamlas({})".format(invoice['vl_szolgaltatasszamlaid']),
                                    "vl_dij_tipus":100000002,
                                    "vl_Berl_Uzem_dij_kedv_Eredeti":0,
                                    "vl_Termek@odata.bind":"products({})".format("9a2848b9-356f-ec11-8943-000d3a46c88e"),
                                    "vl_Berleti_Uzemeltetesi_dij_deviza":100000000,
                                    "vl_Berleti_Uzemeltetesi_dij":contractLine['vl_fix_berleti_dij']
                                }
                                if method != 'test':
                                    if ro.createRecord(data,'vl_szolgaltatasszamlasors',config) >= 400:
                                        print("Nem sikerült a létrehozás!")
                            print('A következő sor fix díjas:')
                            print('Számlázandó szerződés:' + str(contract['vl_szerzodesszam']) + ' Számlázandó sor:' + str(contractLine['vl_name'])+ ' Összeg:' + str(contractLine['vl_fix_berleti_dij'])+"Ft")
                            response[0]=response[0]+"\nA következő sor fix díjas:"
                            response[0]=response[0]+"\nSzámlázandó szerződés:" + str(contract['vl_szerzodesszam']) + ' Számlázandó sor:' + str(contractLine['vl_name'])+ ' Összeg:' + str(contractLine['vl_fix_berleti_dij'])+"Ft"
                            if contractLine['vl_fix_berleti_dij'] is not None and contractLine['vl_fix_berleti_dij']!='None':
                                    billPartner+=int(contractLine['vl_fix_berleti_dij'])
                            else: billPartner+=0
                            # 
                            # HA limites szerzodes -- Limit elerve
                            # 
                        else:
                            if commandName=="monthly":
                                data = {
                                    "vl_vl_Szerzodo_Partner@odata.bind":"accounts({})".format(contract['_vl_ugyfel_value']),
                                    "vl_Name":contractLine['vl_name'],
                                    "vl_POS_2@odata.bind":"vl_poses({})".format(posDictionary[contractLine['vl_name']]),
                                    "vl_Szolgaltatas_szamla@odata.bind":"vl_szolgaltatasszamlas({})".format(invoice['vl_szolgaltatasszamlaid']),
                                    "vl_dij_tipus":100000000,
                                    "vl_Berl_Uzem_dij_kedv_Eredeti":100,
                                    "vl_Termek@odata.bind":"products({})".format("9a2848b9-356f-ec11-8943-000d3a46c88e"),
                                    "vl_Berleti_Uzemeltetesi_dij_deviza":deviza,
                                    "vl_Berleti_Uzemeltetesi_dij":totalFee
                                }
                                if method != 'test':
                                    if ro.createRecord(data,'vl_szolgaltatasszamlasors',config) >= 400:
                                        print("Nem sikerült a létrehozás!")
                            print('A következő sor elérte a limitet:')
                            print('Számlázandó szerződés:' + str(contract['vl_szerzodesszam']) + ' Számlázandó sor:' + str(contractLine['vl_name'])+ ' Összeg: 0 Ft')
                            response[0]=response[0]+"\nA következő sor elérte a limitet:"
                            response[0]=response[0]+"\nSzámlázandó szerződés:" + str(contract['vl_szerzodesszam']) + ' Számlázandó sor:' + str(contractLine['vl_name'])+ ' Összeg: 0 Ft'
                # 
                # Teljes limitet nem erte el
                # 
                elif not missing:
                    for contractLine in listedContractLines:
                        # 
                        # Ha nem limites szerzodessor -- Fix dijas
                        # 
                        if contractLine['vl_limittel_erintett_szerzodessor'] is False:
                            if commandName=="monthly":
                                data = {
                                    "vl_vl_Szerzodo_Partner@odata.bind":"accounts({})".format(contract['_vl_ugyfel_value']),
                                    "vl_Name":contractLine['vl_name'],
                                    "vl_Szolgaltatas_szamla@odata.bind":"vl_szolgaltatasszamlas({})".format(invoice['vl_szolgaltatasszamlaid']),
                                    "vl_dij_tipus":100000002,
                                    "vl_Berl_Uzem_dij_kedv_Eredeti":0,
                                    "vl_Termek@odata.bind":"products({})".format("9a2848b9-356f-ec11-8943-000d3a46c88e"),
                                    "vl_Berleti_Uzemeltetesi_dij_deviza":100000000,
                                    "vl_Berleti_Uzemeltetesi_dij":contractLine['vl_fix_berleti_dij']
                                }
                                if method != 'test':
                                    if ro.createRecord(data,'vl_szolgaltatasszamlasors',config) >= 400:
                                        print("Nem sikerült a létrehozás!")
                            print('A következő sor fix díjas:')
                            print('Számlázandó szerződés:' + str(contract['vl_szerzodesszam']) + ' Számlázandó sor:' + str(contractLine['vl_name'])+ ' Összeg:' + str(contractLine['vl_fix_berleti_dij'])+"Ft")
                            response[0]=response[0]+"\nA következő sor fix díjas:"
                            response[0]=response[0]+"\nSzámlázandó szerződés:" + str(contract['vl_szerzodesszam']) + ' Számlázandó sor:' + str(contractLine['vl_name'])+ ' Összeg:' + str(contractLine['vl_fix_berleti_dij'])+"Ft"
                            if contractLine['vl_fix_berleti_dij'] is not None and contractLine['vl_fix_berleti_dij']!='None':
                                    billPartner+=int(contractLine['vl_teljes_berleti_dij_huf'])
                        # 
                        # Ha van megadva limit koztes ertek
                        # 
                        elif contractLine['vl_limit_koztes_ertek']:
                            # 
                            # Ha elerte a koztes limitet -- Koztes limit elerve
                            # 
                            if soldKg >= float(contractLine['vl_limit_erteke'])*limitKg and soldUnit >= float(contractLine['vl_limit_erteke'])*limitUnit and soldHuf >= float(contractLine['vl_limit_erteke'])*limitHuf:
                                print('A következő sor elérte a köztes limitet:')
                                response[0]=response[0]+"\nA következő sor elérte a köztes limitet:"
                                if contractLine['vl_koztes_limit_berleti_dija_huf'] is not None and contractLine['vl_koztes_limit_berleti_dija_huf']!='None':
                                    deviza=100000000
                                    runningFee=int(contractLine['vl_teljes_berleti_dij_huf'])
                                    billPartner+=int(contractLine['vl_koztes_limit_berleti_dija_huf'])
                                    print('Számlázandó szerződés:' + str(contract['vl_szerzodesszam']) + ' Számlázandó sor:' + str(contractLine['vl_name'])+' Összeg:' + str(contractLine['vl_koztes_limit_berleti_dija_huf'])+"Ft")
                                    response[0]=response[0]+"\nSzámlázandó szerződés:" + str(contract['vl_szerzodesszam']) + ' Számlázandó sor:' + str(contractLine['vl_name'])+' Összeg:' + str(contractLine['vl_koztes_limit_berleti_dija_huf'])+"Ft"
                                elif contractLine['vl_koztes_limit_berleti_dija_eur'] is not None and contractLine['vl_koztes_limit_berleti_dija_eur']!='None':
                                    deviza=100000001
                                    print('Számlázandó szerződés:' + str(contract['vl_szerzodesszam']) + ' Számlázandó sor:' + str(contractLine['vl_name'])+' Összeg:' + str(contractLine['vl_koztes_limit_berleti_dija_eur'])+"Eur")
                                    response[0]=response[0]+"\nSzámlázandó szerződés:" + str(contract['vl_szerzodesszam']) + ' Számlázandó sor:' + str(contractLine['vl_name'])+' Összeg:' + str(contractLine['vl_koztes_limit_berleti_dija_eur'])+"Eur"
                                    billPartner2+=int(contractLine['vl_koztes_limit_berleti_dija_eur'])
                                    runningFee=int(contractLine['vl_teljes_berleti_dij_eur'])
                                if commandName=="monthly":
                                    data = {
                                    "vl_vl_Szerzodo_Partner@odata.bind":"accounts({})".format(contract['_vl_ugyfel_value']),
                                    "vl_Name":contractLine['vl_name'],
                                    "vl_POS_2@odata.bind":"vl_poses({})".format(posDictionary[contractLine['vl_name']]),
                                    "vl_Szolgaltatas_szamla@odata.bind":"vl_szolgaltatasszamlas({})".format(invoice['vl_szolgaltatasszamlaid']),
                                    "vl_dij_tipus":100000001,
                                    "vl_Berl_Uzem_dij_kedv_Eredeti":int(100*float(contractLine['vl_limit_erteke'])),
                                    "vl_Termek@odata.bind":"products({})".format("9a2848b9-356f-ec11-8943-000d3a46c88e"),
                                    "vl_Berleti_Uzemeltetesi_dij_deviza":deviza,
                                    "vl_Berleti_Uzemeltetesi_dij":runningFee
                                }
                                    if method != 'test':
                                        if ro.createRecord(data,'vl_szolgaltatasszamlasors',config) >= 400:
                                            print("Nem sikerült a létrehozás!")
                            # 
                            # Ha nem erte el a koztes limitet sem -- Teljes dij
                            # 
                            else:
                                print('A következő sor nem érte el egyik limitet sem:')
                                response[0]=response[0]+"\nA következő sor nem érte el egyik limitet sem:"
                                if contractLine['vl_teljes_berleti_dij_huf'] is not None and contractLine['vl_teljes_berleti_dij_huf']!='None':
                                    deviza=100000000
                                    print('Számlázandó szerződés:' + str(contract['vl_szerzodesszam']) + ' Számlázandó sor:' + str(contractLine['vl_name'])+ ' Összeg:' + str(contractLine['vl_teljes_berleti_dij_huf'])+"Ft")
                                    response[0]=response[0]+"\nSzámlázandó szerződés:" + str(contract['vl_szerzodesszam']) + ' Számlázandó sor:' + str(contractLine['vl_name'])+ ' Összeg:' + str(contractLine['vl_teljes_berleti_dij_huf'])+"Ft"
                                    billPartner+=int(contractLine['vl_teljes_berleti_dij_huf'])
                                    runningFee=int(contractLine['vl_teljes_berleti_dij_huf'])
                                if contractLine['vl_teljes_berleti_dij_eur'] is not None and  contractLine['vl_teljes_berleti_dij_eur']!='None':
                                    runningFee=int(contractLine['vl_teljes_berleti_dij_eur'])
                                    deviza=100000001
                                    print('Számlázandó szerződés:' + str(contract['vl_szerzodesszam']) + ' Számlázandó sor:' + str(contractLine['vl_name'])+ ' Összeg:' + str(contractLine['vl_teljes_berleti_dij_eur'])+"Eur")
                                    response[0]=response[0]+"\nSzámlázandó szerződés:" + str(contract['vl_szerzodesszam']) + ' Számlázandó sor:' + str(contractLine['vl_name'])+ ' Összeg:' + str(contractLine['vl_teljes_berleti_dij_eur'])+"Eur"
                                    billPartner2+=int(contractLine['vl_teljes_berleti_dij_eur'])
                                if commandName=="monthly":
                                    data = {
                                    "vl_vl_Szerzodo_Partner@odata.bind":"accounts({})".format(contract['_vl_ugyfel_value']),
                                    "vl_Name":contractLine['vl_name'],
                                    "vl_POS_2@odata.bind":"vl_poses({})".format(posDictionary[contractLine['vl_name']]),
                                    "vl_Szolgaltatas_szamla@odata.bind":"vl_szolgaltatasszamlas({})".format(invoice['vl_szolgaltatasszamlaid']),
                                    "vl_dij_tipus":100000003,
                                    "vl_Berl_Uzem_dij_kedv_Eredeti":0,
                                    "vl_Termek@odata.bind":"products({})".format("9a2848b9-356f-ec11-8943-000d3a46c88e"),
                                    "vl_Berleti_Uzemeltetesi_dij_deviza":deviza,
                                    "vl_Berleti_Uzemeltetesi_dij":runningFee
                                }
                                    if method != 'test':
                                        if ro.createRecord(data,'vl_szolgaltatasszamlasors',config) >= 400:
                                            print("Nem sikerült a létrehozás!")
                        # 
                        # Ha nem ert el limitet (se teljes, se koztes) -- Teljes dij
                        # 
                        else:
                            print('A következő sor nem érte el egyik limitet sem:')
                            response[0]=response[0]+"\nA következő sor nem érte el egyik limitet sem:"
                            if contractLine['vl_teljes_berleti_dij_huf'] is not None and contractLine['vl_teljes_berleti_dij_huf']!='None':
                                deviza=100000000
                                runningFee=int(contractLine['vl_teljes_berleti_dij_huf'])
                                billPartner+=int(contractLine['vl_teljes_berleti_dij_huf'])
                                response[0]=response[0]+"\nSzámlázandó szerződés:" + str(contract['vl_szerzodesszam']) + ' Számlázandó sor:' + str(contractLine['vl_name'])+ ' Összeg:' + str(contractLine['vl_teljes_berleti_dij_huf'])+"Ft"
                                print('Számlázandó szerződés:' + str(contract['vl_szerzodesszam']) + ' Számlázandó sor:' + str(contractLine['vl_name'])+ ' Összeg:' + str(contractLine['vl_teljes_berleti_dij_huf'])+"Ft")
                            if contractLine['vl_teljes_berleti_dij_eur'] is not None and contractLine['vl_teljes_berleti_dij_eur']!='None':
                                deviza=100000001
                                runningFee=int(contractLine['vl_teljes_berleti_dij_eur'])
                                billPartner2+=int(contractLine['vl_teljes_berleti_dij_eur'])
                                response[0]=response[0]+"\nSzámlázandó szerződés:" + str(contract['vl_szerzodesszam']) + ' Számlázandó sor:' + str(contractLine['vl_name'])+ ' Összeg:' + str(contractLine['vl_teljes_berleti_dij_eur'])+"Eur"
                                print('Számlázandó szerződés:' + str(contract['vl_szerzodesszam']) + ' Számlázandó sor:' + str(contractLine['vl_name'])+ ' Összeg:' + str(contractLine['vl_teljes_berleti_dij_eur'])+"Eur")
                            if commandName=="monthly":
                                data = {
                                    "vl_vl_Szerzodo_Partner@odata.bind":"accounts({})".format(contract['_vl_ugyfel_value']),
                                    "vl_Name":contractLine['vl_name'],
                                    "vl_POS_2@odata.bind":"vl_poses({})".format(posDictionary[contractLine['vl_name']]),
                                    "vl_Szolgaltatas_szamla@odata.bind":"vl_szolgaltatasszamlas({})".format(invoice['vl_szolgaltatasszamlaid']),
                                    "vl_dij_tipus":100000003,
                                    "vl_Berl_Uzem_dij_kedv_Eredeti":0,
                                    "vl_Termek@odata.bind":"products({})".format("9a2848b9-356f-ec11-8943-000d3a46c88e"),
                                    "vl_Berleti_Uzemeltetesi_dij_deviza":deviza,
                                    "vl_Berleti_Uzemeltetesi_dij":runningFee
                                }
                                if method != 'test':
                                    if ro.createRecord(data,'vl_szolgaltatasszamlasors',config) >= 400:
                                        print("Nem sikerült a létrehozás!")
        dailyData[0]=soldKg
        dailyData[1]=soldHuf
        dailyData[2]=soldUnit
        dailyData[3]=limitKg
        dailyData[4]=limitHuf
        dailyData[5]=limitUnit
        dailyData[6]=billPartner
        dailyData[7]=billPartner2
        if commandName=="daily" and limitLevel==100000000:
            print(contract['vl_szerzodesszam'])
            print("Eladott Kg: "+str(dailyData[0]))
            print("Eladott Összeg: "+str(dailyData[1]))
            print("Eladott Mennyiség: "+str(dailyData[2]))
            print("Limit Kg: "+str(dailyData[3]))
            print("Limit Összeg: "+str(dailyData[4]))
            print("Limit Mennyiség: "+str(dailyData[5]))
            print("Számlázandó összeg (Forint): "+str(dailyData[6]))
            print("Számlázandó összeg (Euro): "+str(dailyData[7]))
            response[0]=response[0]+"\nSzerződésszáma: "+contract['vl_szerzodesszam']
            response[0]=response[0]+"\nEladott Kg: "+str(dailyData[0])
            response[0]=response[0]+"\nEladott Összeg: "+str(dailyData[1])
            response[0]=response[0]+"\nEladott Mennyiség: "+str(dailyData[2])
            response[0]=response[0]+"\nLimit Kg: "+str(dailyData[3])
            response[0]=response[0]+"\nLimit Összeg: "+str(dailyData[4])
            response[0]=response[0]+"\nLimit Mennyiség: "+str(dailyData[5])
            response[0]=response[0]+"\nSzámlázandó összeg (Forint): "+str(dailyData[6])
            response[0]=response[0]+"\nSzámlázandó összeg (Euro): "+str(dailyData[7])
            data = {
                "vl_eladott_kgs_szerz":dailyData[0],
                "vl_eladott_osszeg_szerz":dailyData[1],
                "vl_eladott_mennyiseg_szerz":dailyData[2],
                "vl_limit_kg_szerz":dailyData[3],
                "vl_limit_osszeg_szerz":dailyData[4],
                "vl_limit_mennyiseg_szerz":dailyData[5],
                "vl_vegosszeg_huf":str(dailyData[6]),
                "vl_vegosszeg_eur":str(dailyData[7]),
                "vl_hatralevo_kg_szerz":dailyData[3]-dailyData[0],
                "vl_hatralevo_osszeg_szerz":dailyData[4]-dailyData[1],
                "vl_hatralevo_mennyiseg_szerz":dailyData[5]-dailyData[6],
                "vl_akt_sszaml_per_kezd_szerz":currPeriodStart,
                "vl_akt_sszaml_per_vege_szerz":currPeriodEnd,
            }
            if method != 'test':
                ro.updateRecord(data,contract['vl_szerzodesekid'],'vl_szerzodeseks',config)
        if commandName=="monthly" and listedContractLines!=-1 and invoice is not None:
            percentage=0
            divider=0
            for i in [0,1,2]:
                if dailyData[i] > 0: 
                    percentage+=dailyData[i+3]/dailyData[i]
                    divider+=1
            if divider >0:
                data= {
                    "vl_Vegosszeghuf":dailyData[6],
                    "vl_Vegosszegeur":dailyData[7],
                    "vl_limittelj_szazaleka":percentage/divider,
                    "vl_Megjegyzes":json.dumps(missingData | missingDataLine),
                    #"vl_megjegyzes":len(json.dumps(missingData | missingDataLine))>2 and "Hiányos" or "Nem hiányos",
                    'vl_Szamla_statusz_oka':100000005
                }
            else:
                data= {
                    "vl_Vegosszeghuf":dailyData[6],
                    "vl_Vegosszegeur":dailyData[7],
                    "vl_limittelj_szazaleka":0,
                    "vl_Megjegyzes":json.dumps(missingData | missingDataLine),
                    #"vl_Megjegyzes":len(json.dumps(missingData | missingDataLine))>2 and "Hiányos" or "Nem hiányos",
                    'vl_Szamla_statusz_oka':100000005
                }
            if method != 'test':
                ro.updateRecord(data,invoice['vl_szolgaltatasszamlaid'],'vl_szolgaltatasszamlas',config)
    dailyData[0]=soldKgPartner
    dailyData[1]=soldHufPartner
    dailyData[2]=soldUnitPartner
    dailyData[3]=limitKgPartner
    dailyData[4]=limitHufPartner
    dailyData[5]=limitUnitPartner
    dailyData[6]=billPartner
    dailyData[7]=billPartner2
if __name__ == "__main__":
    main()