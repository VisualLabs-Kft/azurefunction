import datetime
from math import prod
from dateutil.relativedelta import relativedelta
import logging
import json
import connectToCDS as ctc
import recordOperations as ro
import requests
import http.client

auth_url = 'https://login.microsoftonline.com/2ec83d1c-69d7-4d7c-a4f8-959cde9d1b46/oauth2/v2.0/token'
client_id = 'd6b218d6-f97d-4917-88f9-3cf7c33aa4bc'
scope = 'https://api.businesscentral.dynamics.com/.default'
client_secret = "VIK7Q~p3T20CCPQ~f8-rLRGcIZNZz93c-QlcN"

def main():

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
    listedCustomerGroups = ro.queryRecords(filt, 'vl_limit_megadasi_helye_szintje', 'vl_vevocsoports', config)
    if listedCustomerGroups != -1:
        limit_on_customergroup(listedCustomerGroups,config)
    
    filt = {
            'filter1': {
                'field': 'vl_limitfigyeles_szintje_partner',
                'operator': 'eq',
                'value': 'true'
            }
    }
    print("Listing Customers with limit handle level")
    listedCustomers = ro.queryRecords(filt, 'vl_limitfigyeles_szintje_partner,_vl_vevocsoport_partner_value,_parentaccountid_value', 'accounts', config)
    #Szerződések szűréséhez, ha kikerülne a partner a listából mert van fölötte lévő szint
    listedCustomersForContracts = listedCustomers[:]
    if listedCustomers != -1:
        if listedCustomerGroups != -1:
            for customer in listedCustomers:
                if customer['_parentaccountid_value'] is None:
                    pass
                else:
                    parentCustomer=ro.getRecord(customer['_parentaccountid_value'],'accounts',config)
                    if parentCustomer['vl_limitfigyeles_szintje_partner'] is True:
                        listedCustomers.remove(customer)
            for customer in listedCustomers:
                for group in listedCustomerGroups:
                    if customer['_vl_vevocsoport_partner_value'] == group['vl_vevocsoportid']:
                        listedCustomers.remove(customer)
        limit_on_customer(listedCustomers,config)
    
    filt = {
            'filter1': {
                'field': 'vl_limit_figyeles_szintje_szerz',
                'operator': 'eq',
                'value': 'true'
            }
    }
    print("Listing Contracts with limit handle level")
    listedContracts = ro.queryRecords(filt, 'vl_szerzodesszam,vl_limit_figyeles_szintje_szerz,_vl_ugyfel_value,vl_szamlazasi_periodus_kezdete_szerzodes,vl_szamlazasi_periodus_szerzodes,vl_szerzodes_kategoria,vl_szerzodes_lejarata', 'vl_szerzodeseks', config)
    if listedContracts != -1:
        if listedCustomersForContracts != -1:
            for contract in listedContracts:
                for customer in listedCustomersForContracts:
                    if customer['accountid'] == contract['_vl_ugyfel_value']:
                        listedContracts.remove(contract)
        limit_on_contract(listedContracts,config)


#nem jó így + beszéljük át
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
    elif periodStart <= posVatDate and posVatDate <= periodEnd: 
        if linePeriodStart <= posVatDate and posVatDate <= linePeriodEnd and posVatDate >= datetime.datetime.today()-relativedelta(days=periodDays): 
            return True
        else: return False
    else: return False

def AD_calculate(contractLine,contract,periodDays,bcHead,bcLine,pos,contractProductsKg,contractProductsHuf,contractProductsUnit):
    if contractLine['vl_limittel_erintett_szerzodessor'] is True:
        #ha nincs megadva számlázás vége akkor a szerződés vége lesz az ??mivan ha ez sincs?
        if contractLine['vl_szamlazas_vege'] is None: 
            if contract['vl_szerzodes_lejarata'] is None:
                szamlazasVege=""
            else:
                szamlazasVege = datetime.datetime.strptime(contract['vl_szerzodes_lejarata'],'%Y-%m-%d')
        else:
            szamlazasVege = datetime.datetime.strptime(contractLine['vl_szamlazas_vege'].split('T')[0],'%Y-%m-%d')
        correction=calculate_correction(datetime.datetime.strptime(contract['vl_szamlazasi_periodus_kezdete_szerzodes'].split('T')[0],'%Y-%m-%d'),contract['vl_szerzodes_lejarata'],periodDays,datetime.datetime.strptime(contractLine['vl_szamlazas_kezdete'].split('T')[0],'%Y-%m-%d'),szamlazasVege)
        print("Korrekció: "+str(correction))
        postedSalesInvoiceLines=[]
        postedSalesInvoiceHeads=[]
        #Eladási sorok kilistázása a megadott pos-hez
        for line in bcLine:
            ok=False
            if line['Shortcut_Dimension_2_Code']==pos['vl_pos_id']:
                if contractLine['vl_limitertek_alapja'] == 100000002:
                    ok=True
                else:
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
                            if drop_pos(contract['vl_szamlazasi_periodus_kezdete_szerzodes'],contract['vl_szerzodes_lejarata'],contractLine['vl_szamlazas_kezdete'],contractLine['vl_szamlazas_vege'],periodDays,head['Shipment_Date']):
                                postedSalesInvoiceLines.append(line)
                                postedSalesInvoiceHeads.append(head)
        #Eladott mennyiségek összegzése
        #kg
        if postedSalesInvoiceLines:
            token = get_token(auth_url, client_id, scope, client_secret)
            access_token = token[0]['access_token']
            header_token = {"Authorization": "Bearer {}".format(access_token)}
            products = requests.get(url="https://api.businesscentral.dynamics.com/v2.0/2ec83d1c-69d7-4d7c-a4f8-959cde9d1b46/Development/ODataV4/Company('Dallmayr')/VL_Item_Card", headers=header_token).json()["value"]
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

def limit_on_customergroup(listedCustomerGroups,config):
    pass

def limit_on_customer(listedCustomers,config):
    pass

def limit_on_contract(listedContracts,config):
    print("-------------------CONNECT TO BUSINESS CENTRAL---------------------")
    auth_url = 'https://login.microsoftonline.com/2ec83d1c-69d7-4d7c-a4f8-959cde9d1b46/oauth2/v2.0/token'
    client_id = 'd6b218d6-f97d-4917-88f9-3cf7c33aa4bc'
    scope = 'https://api.businesscentral.dynamics.com/.default'
    client_secret = "VIK7Q~p3T20CCPQ~f8-rLRGcIZNZz93c-QlcN"
    token = get_token(auth_url, client_id, scope, client_secret)
    access_token = token[0]['access_token']
    header_token = {"Authorization": "Bearer {}".format(access_token)}
    bcHead = requests.get(url="https://api.businesscentral.dynamics.com/v2.0/2ec83d1c-69d7-4d7c-a4f8-959cde9d1b46/Development/ODataV4/Company('Dallmayr')/VL_PostedSalesInvoices", headers=header_token).json()["value"]
    bcLine = requests.get(url="https://api.businesscentral.dynamics.com/v2.0/2ec83d1c-69d7-4d7c-a4f8-959cde9d1b46/Development/ODataV4/Company('Dallmayr')/VL_PostedSalesInvoiceLines",headers=header_token).json()["value"]
    print("DONE")
    print("-------------------------LIMIT ON CONTRACT--------------------------")
    for contract in listedContracts:
        print('!!!! Szerződés: '+str(contract['vl_szerzodesszam'])+' !!!!')
        #teljesítési periodus megadása
        #nem jó így, most mindig az első hónap hossza lesz, mi kéne helyette?
        if contract['vl_szamlazasi_periodus_szerzodes'] == 100000000:
            periodDays=(datetime.datetime.strptime(contract['vl_szamlazasi_periodus_kezdete_szerzodes'].split('T')[0],'%Y-%m-%d')+relativedelta(months=+1)-datetime.datetime.strptime(contract['vl_szamlazasi_periodus_kezdete_szerzodes'].split('T')[0],'%Y-%m-%d')).days
        elif contract['vl_szamlazasi_periodus_szerzodes'] == 100000001:
            periodDays=(datetime.datetime.strptime(contract['vl_szamlazasi_periodus_kezdete_szerzodes'].split('T')[0],'%Y-%m-%d')+relativedelta(months=+3)-datetime.datetime.strptime(contract['vl_szamlazasi_periodus_kezdete_szerzodes'].split('T')[0],'%Y-%m-%d')).days
        elif contract['vl_szamlazasi_periodus_szerzodes'] == 100000002:
            periodDays=(datetime.datetime.strptime(contract['vl_szamlazasi_periodus_kezdete_szerzodes'].split('T')[0],'%Y-%m-%d')+relativedelta(months=+6)-datetime.datetime.strptime(contract['vl_szamlazasi_periodus_kezdete_szerzodes'].split('T')[0],'%Y-%m-%d')).days
        elif contract['vl_szamlazasi_periodus_szerzodes'] == 100000003:
            periodDays=(datetime.datetime.strptime(contract['vl_szamlazasi_periodus_kezdete_szerzodes'].split('T')[0],'%Y-%m-%d')+relativedelta(months=+12)-datetime.datetime.strptime(contract['vl_szamlazasi_periodus_kezdete_szerzodes'].split('T')[0],'%Y-%m-%d')).days
        else:
            periodDays=(contract['vl_szerzodes_lejarata']-contract['vl_szamlazasi_periodus_kezdete_szerzodes']).days
        print('Periódus hossza: '+str(periodDays))
        #Szerződés sorainak listázása
        filt = {
            'filter1': {
                'field': '_vl_szerzodes_value',
                'operator': 'eq',
                'value': contract['vl_szerzodesekid']
            }
        }
        listedContractLines = ro.queryRecords(filt, 'vl_koztes_limit_berleti_dija_huf,vl_teljes_berleti_dij_huf,vl_fix_berleti_dij,vl_name,_vl_szerzodes_value,vl_limittel_erintett_szerzodessor,vl_limitertek_alapja,vl_szamlazas_kezdete,vl_szamlazas_vege,_vl_kapcsolodo_pos_value,vl_szamlazo,vl_limit_erteke,vl_limit_koztes_ertek,vl_berl_dij_min_nem_telj_eseten_fix,vl_berl_dij_min_telj_eseten_huf,vl_berl_dij_max_feletti_telj_eseten', 'vl_szerzodessorais', config)
        
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
                    'field': '_vl_szerzodesek_limit_termek_value',
                    'operator': 'eq',
                    'value': contract['vl_szerzodesekid']
                }
            }
            contractProducts = ro.queryRecords(filt, '_vl_szerzodesek_limit_termek_value,vl_limitvizsgalatialap,_vl_kapcsolodo_termek_value', 'vl_limittermeksorais', config)
            contractProductsKg=[]
            contractProductsHuf=[]
            contractProductsUnit=[]
            for product in contractProducts:
                if product['vl_limitvizsgalatialap']==100000000:
                    contractProductsKg.append(ro.getRecord(product['_vl_kapcsolodo_termek_value'],'products',config))
                elif product['vl_limitvizsgalatialap']==100000001:
                    contractProductsHuf.append(ro.getRecord(product['_vl_kapcsolodo_termek_value'],'products',config))
                else: contractProductsUnit.append(ro.getRecord(product['_vl_kapcsolodo_termek_value'],'products',config))

            
            #Horeca vagy Vending szerződés
            posDictionary={}
            if contract['vl_szerzodes_kategoria'] == 100000000 or contract['vl_szerzodes_kategoria'] == 1000000001:
                #Havi számlázási periódus
                #if contract['vl_szamlazasi_periodus_szerzodes'] == 100000000:
                    for contractLine in listedContractLines:
                        # Számlázó Dallmayr
                        if contractLine['vl_szamlazo'] == 100000001:
                            #limites sor
                            if contractLine['vl_limittel_erintett_szerzodessor']==True:
                                print('Szerződés sor: '+contractLine['vl_name'])
                                pos=ro.getRecord(contractLine['_vl_kapcsolodo_pos_value'],'vl_poses',config)
                                posDictionary[contractLine['vl_name']]=pos['vl_pos_id']
                                print('POS: '+str(pos['vl_pos_id']))
                                #Kg
                                if contractLine['vl_limitertek_alapja'] == 100000000:
                                    soldKg+=AD_calculate(contractLine,contract,periodDays,bcHead,bcLine,pos,contractProductsKg,contractProductsHuf,contractProductsUnit)
                                    limitKg+=float(contractLine['vl_limit_erteke'])
                                    if contractLine['vl_limit_koztes_ertek']:
                                        halfLimitKg+=float(contractLine['vl_limit_koztes_ertek'])
                                #Huf
                                elif contractLine['vl_limitertek_alapja'] == 100000001:
                                    soldHuf+=AD_calculate(contractLine,contract,periodDays,bcHead,bcLine,pos,contractProductsKg,contractProductsHuf,contractProductsUnit)
                                    limitHuf+=float(contractLine['vl_limit_erteke'])
                                    if contractLine['vl_limit_koztes_ertek']:
                                        halfLimitHuf+=float(contractLine['vl_limit_koztes_ertek'])
                                #Unit
                                elif contractLine['vl_limitertek_alapja'] == 100000002:
                                    soldUnit+=AD_calculate(contractLine,contract,periodDays,bcHead,bcLine,pos,contractProductsKg,contractProductsHuf,contractProductsUnit)
                                    limitUnit+=float(contractLine['vl_limit_erteke'])
                                    if contractLine['vl_limit_koztes_ertek']:
                                        halfLimitUnit+=float(contractLine['vl_limit_koztes_ertek'])
        if halfLimitHuf:
            halfLimitHuf=limitHuf*(halfLimitHuf/100)
        if halfLimitKg:
            halfLimitKg=limitKg*(halfLimitKg/100)
        if halfLimitUnit:
            halfLimitUnit=limitUnit*(halfLimitUnit/100)
        if listedContractLines != -1:                    
            print('Eladott kg:'+str(soldKg))
            print('Eladott összeg: ' + str(soldHuf))
            print('Eladott mennyiség: '+str(soldUnit))
            print('Limit kg: '+str(limitKg))
            print('Limit összeg: '+str(limitHuf))
            print('Limit mennyiség: '+str(limitUnit))
            print('Köztes limit kg: '+str(halfLimitKg))
            print('Köztes limit összeg: '+str(halfLimitHuf))
            print('Köztes limit mennyiség: '+str(halfLimitUnit))
            id=datetime.datetime.today().timestamp()
            data = {
                #"regardingobjectid@odata.bind": "incidents({})".format(incidentid),
                "vl_eladott_kg":soldKg,
                "vl_eladott_mennyiseg":soldUnit,
                "vl_eladott_osszeg":soldHuf,
                "vl_koztes_limit_kg":halfLimitKg,
                "vl_koztes_limit_mennyiseg":halfLimitUnit,
                "vl_koztes_limit_osszeg":halfLimitHuf,
                "vl_limit_kg":limitKg,
                "vl_limit_mennyiseg":limitUnit,
                "vl_limit_osszeg":limitHuf,
                "vl_periodus":periodDays,
                "vl_limit_szint":100000000,
                "vl_name":contract['vl_szerzodesszam'],
                #ideiglenes id mező
                "vl_korrekcio":id
            }
            if ro.createRecord(data,'vl_szolgaltatasszamlazases',config) > 400:
                print("Nem sikerült a létrehozás!")
            else:
                filt = {
                    'filter1': {
                        'field': 'vl_limit_szint',
                        'operator': 'eq',
                        'value': "100000000"
                    }
                }
                invoices=ro.queryRecords(filt,'vl_korrekcio,vl_name','vl_szolgaltatasszamlazases',config)
                for record in invoices:
                    if int(record['vl_korrekcio'])==int(id) and record['vl_name']==contract['vl_szerzodesszam']:
                        invoice=record
            if soldKg >= limitKg and soldUnit >= limitUnit and soldHuf >= limitHuf:
                for contractLine in listedContractLines:
                    if contractLine['vl_limittel_erintett_szerzodessor'] is False:
                        updateData = {
                            "vl_name":contractLine['vl_name'],
                            "vl_szerzodes":contract['vl_szerzodesszam'],
                            "vl_szolgaltatas_szamlazas@odata.bind":"vl_szolgaltatasszamlazases({})".format(invoice['vl_szolgaltatasszamlazasid']),
                            "vl_dij_tipus":100000002,
                            "vl_szamlazando_osszeg":contractLine['vl_fix_berleti_dij']
                        }
                        if ro.createRecord(updateData,'vl_szolgaltatasszamlazassorais',config) > 400:
                            print("Nem sikerült a létrehozás!")
                        print('A következő sor fix díjas:')
                        print('Számlázandó szerződés:' + str(contract['vl_szerzodesszam']) + ' Számlázandó sor:' + str(contractLine['vl_name'])+ ' Összeg:' + str(int(contractLine['vl_fix_berleti_dij']))+"Ft")
                    else:
                        updateData = {
                            "vl_name":contractLine['vl_name'],
                            "vl_pos":posDictionary[contractLine['vl_name']],
                            "vl_szerzodes":contract['vl_szerzodesszam'],
                            "vl_szolgaltatas_szamlazas@odata.bind":"vl_szolgaltatasszamlazases({})".format(invoice['vl_szolgaltatasszamlazasid']),
                            "vl_dij_tipus":100000000,
                            "vl_szamlazando_osszeg":0
                        }
                        if ro.createRecord(updateData,'vl_szolgaltatasszamlazassorais',config) > 400:
                            print("Nem sikerült a létrehozás!")
                        print('A következő sor elérte a limitet:')
                        print('Számlázandó szerződés:' + str(contract['vl_szerzodesszam']) + ' Számlázandó sor:' + str(contractLine['vl_name'])+ ' Összeg: 0 Ft')
            elif soldKg >= halfLimitKg and soldUnit >= halfLimitUnit and soldHuf >= halfLimitHuf:
                for contractLine in listedContractLines:
                    if contractLine['vl_limittel_erintett_szerzodessor'] is False:
                        updateData = {
                            "vl_name":contractLine['vl_name'],
                            "vl_szerzodes":contract['vl_szerzodesszam'],
                            "vl_szolgaltatas_szamlazas@odata.bind":"vl_szolgaltatasszamlazases({})".format(invoice['vl_szolgaltatasszamlazasid']),
                            "vl_dij_tipus":100000002,
                            "vl_szamlazando_osszeg":contractLine['vl_fix_berleti_dij']
                        }
                        if ro.createRecord(updateData,'vl_szolgaltatasszamlazassorais',config) > 400:
                            print("Nem sikerült a létrehozás!")
                        print('A következő sor fix díjas:')
                        print('Számlázandó szerződés:' + str(contract['vl_szerzodesszam']) + ' Számlázandó sor:' + str(contractLine['vl_name'])+ ' Összeg:' + str(int(contractLine['vl_fix_berleti_dij']))+"Ft")
                    else:
                        updateData = {
                            "vl_name":contractLine['vl_name'],
                            "vl_pos":posDictionary[contractLine['vl_name']],
                            "vl_szerzodes":contract['vl_szerzodesszam'],
                            "vl_szolgaltatas_szamlazas@odata.bind":"vl_szolgaltatasszamlazases({})".format(invoice['vl_szolgaltatasszamlazasid']),
                            "vl_dij_tipus":100000001,
                            "vl_szamlazando_osszeg":contractLine['vl_koztes_limit_berleti_dija_huf']
                        }
                        if ro.createRecord(updateData,'vl_szolgaltatasszamlazassorais',config) > 400:
                            print("Nem sikerült a létrehozás!")
                        print('A következő sor elérte a köztes limitet:')
                        print('Számlázandó szerződés:' + str(contract['vl_szerzodesszam']) + ' Számlázandó sor:' + str(contractLine['vl_name'])+' Összeg:' + str(int(contractLine['vl_koztes_limit_berleti_dija_huf']))+"Ft")
            else:
                for contractLine in listedContractLines:
                    if contractLine['vl_limittel_erintett_szerzodessor'] is False:
                        updateData = {
                            "vl_name":contractLine['vl_name'],
                            "vl_szerzodes":contract['vl_szerzodesszam'],
                            "vl_szolgaltatas_szamlazas@odata.bind":"vl_szolgaltatasszamlazases({})".format(invoice['vl_szolgaltatasszamlazasid']),
                            "vl_dij_tipus":100000002,
                            "vl_szamlazando_osszeg":contractLine['vl_fix_berleti_dij']
                        }
                        if ro.createRecord(updateData,'vl_szolgaltatasszamlazassorais',config) > 400:
                            print("Nem sikerült a létrehozás!")
                        print('A következő sor fix díjas:')
                        print('Számlázandó szerződés:' + str(contract['vl_szerzodesszam']) + ' Számlázandó sor:' + str(contractLine['vl_name'])+ ' Összeg:' + str(int(contractLine['vl_fix_berleti_dij']))+"Ft")
                    else:
                        updateData = {
                            "vl_name":contractLine['vl_name'],
                            "vl_pos":posDictionary[contractLine['vl_name']],
                            "vl_szerzodes":contract['vl_szerzodesszam'],
                            "vl_szolgaltatas_szamlazas@odata.bind":"vl_szolgaltatasszamlazases({})".format(invoice['vl_szolgaltatasszamlazasid']),
                            "vl_dij_tipus":100000003,
                            "vl_szamlazando_osszeg":contractLine['vl_teljes_berleti_dij_huf']
                        }
                        if ro.createRecord(updateData,'vl_szolgaltatasszamlazassorais',config) > 400:
                            print("Nem sikerült a létrehozás!")
                        print('A következő sor nem érte el egyik limitet sem:')
                        print('Számlázandó szerződés:' + str(contract['vl_szerzodesszam']) + ' Számlázandó sor:' + str(contractLine['vl_name'])+ ' Összeg:' + str(int(contractLine['vl_teljes_berleti_dij_huf']))+"Ft")

if __name__ == "__main__":
    main()