import datetime
from math import prod
from dateutil.relativedelta import relativedelta
import logging
import json
from . import connectToCDS as ctc
from . import recordOperations as ro
import requests
import http.client
import azure.functions as func

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
logging.info("-------------------CONNECT TO BUSINESS CENTRAL---------------------")
auth_url = 'https://login.microsoftonline.com/2ec83d1c-69d7-4d7c-a4f8-959cde9d1b46/oauth2/v2.0/token'
client_id = 'd6b218d6-f97d-4917-88f9-3cf7c33aa4bc'
scope = 'https://api.businesscentral.dynamics.com/.default'
client_secret = "VIK7Q~p3T20CCPQ~f8-rLRGcIZNZz93c-QlcN"
token = get_token(auth_url, client_id, scope, client_secret)
access_token = token[0]['access_token']
header_token = {"Authorization": "Bearer {}".format(access_token)}
bcHead = requests.get(url="https://api.businesscentral.dynamics.com/v2.0/2ec83d1c-69d7-4d7c-a4f8-959cde9d1b46/Development/ODataV4/Company('Dallmayr')/VL_PostedSalesInvoices", headers=header_token).json()["value"]
bcLine = requests.get(url="https://api.businesscentral.dynamics.com/v2.0/2ec83d1c-69d7-4d7c-a4f8-959cde9d1b46/Development/ODataV4/Company('Dallmayr')/VL_PostedSalesInvoiceLines",headers=header_token).json()["value"]



def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    req_body = req.get_json()
    commandName = req_body.get('name')
    logging.info(commandName)
    # Connect to Dataverse
    logging.info("\n ---------------------------------- CONNECT TO DATAVERSE ---------------------------------- ")
    logging.info("Creating config file!")
    config = ctc.getConfig() # defines config data
    config["requestheader"] = {
        'Authorization': 'Bearer ' + ctc.connect_to_cds(), # 'Bearer' + access_token
        'Content-Type': 'application/json',
        'OData-MaxVersion': '4.0',
        'OData-Version': '4.0'
    }

    logging.info("\n ---------------------------------- CHECK LIMIT HANDLING LEVEL ---------------------------------- ")

    filt = {
            'filter1': {
                'field': 'vl_limit_megadasi_helye_szintje',
                'operator': 'eq',
                'value': 'true'
            }
    }
    logging.info("Listing CustomerGroups with limit handle level")
    listedCustomerGroups = ro.queryRecords(filt, 'vl_limit_megadasi_helye_szintje,vl_name', 'vl_vevocsoports', config)
    if listedCustomerGroups != -1:
        limit_on_customergroup(listedCustomerGroups,commandName,config)
    
    filt = {
            'filter1': {
                'field': 'vl_limitfigyeles_szintje_partner',
                'operator': 'eq',
                'value': 'true'
            }
    }
    logging.info("Listing Customers with limit handle level")
    listedCustomers = ro.queryRecords(filt, 'name,vl_limitfigyeles_szintje_partner,_vl_vevocsoport_partner_value,_parentaccountid_value', 'accounts', config)
    #Szerződések szűréséhez, ha kikerülne a partner a listából mert van fölötte lévő szint
    listedCustomersForContracts = listedCustomers[:]
    if listedCustomers != -1:
        if listedCustomerGroups != -1:
            for customer in list(listedCustomers):
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
        limit_on_customer(listedCustomers,commandName,100000001,[0,0,0,0,0,0,0],config)
    
    filt = {
            'filter1': {
                'field': 'vl_limit_figyeles_szintje_szerz',
                'operator': 'eq',
                'value': 'true'
            }
    }
    logging.info("Listing Contracts with limit handle level")
    listedContracts = ro.queryRecords(filt, '_vl_szamlazasi_partner_szerzodes_value,vl_szerzodes_statusza,vl_jovahagyas_statusza,vl_szerzodesszam,vl_limit_figyeles_szintje_szerz,_vl_ugyfel_value,vl_szamlazasi_periodus_kezdete_szerzodes,vl_szamlazasi_periodus_szerzodes,vl_szerzodes_kategoria,vl_szerzodes_lejarata', 'vl_szerzodeseks', config)
    if listedContracts != -1:
        if listedCustomersForContracts != -1:
            for contract in list(listedContracts):
                if contract['vl_szerzodes_statusza']!=100000003 or contract['vl_jovahagyas_statusza']!=True:
                    listedContracts.remove(contract)
                else:
                    for customer in listedCustomersForContracts:
                        if customer['accountid'] == contract['_vl_ugyfel_value']:
                            listedContracts.remove(contract)
        limit_on_contract(listedContracts,commandName,100000000,[0,0,0,0,0,0,0],config)
    return func.HttpResponse("Sikeres futás")


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
                szamlazasVege = datetime.datetime.strptime(str(contract['vl_szerzodes_lejarata']).split('T')[0],'%Y-%m-%d')
        else:
            szamlazasVege = datetime.datetime.strptime(contractLine['vl_szamlazas_vege'].split('T')[0],'%Y-%m-%d')
        if contractLine['vl_szamlazas_kezdete'] is None:
            billingStart=contract['vl_szamlazasi_periodus_kezdete_szerzodes']
        else: billingStart=contractLine['vl_szamlazas_kezdete']
        correction=calculate_correction(datetime.datetime.strptime(contract['vl_szamlazasi_periodus_kezdete_szerzodes'].split('T')[0],'%Y-%m-%d'),datetime.datetime.strptime(contract['vl_szerzodes_lejarata'].split('T')[0],'%Y-%m-%d'),periodDays,datetime.datetime.strptime(billingStart.split('T')[0],'%Y-%m-%d'),szamlazasVege)
        logging.info("Korrekció: "+str(correction))
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
            

            


            


def limit_on_customergroup(listedCustomerGroups,commandName,config):
    for group in listedCustomerGroups:
        #[eladottKg,eladottOsszeg,eladottMennyiseg,limitKg,limitOsszeg,limitMennyiseg,vegosszeg]
        dailyDataVcs=[0,0,0,0,0,0,0]
        filt = {
            'filter1': {
                'field': '_vl_vevocsoport_partner_value',
                'operator': 'eq',
                'value': group['vl_vevocsoportid']
            }
        }
        listedCustomers = ro.queryRecords(filt, 'name,vl_limitfigyeles_szintje_partner,_vl_vevocsoport_partner_value,_parentaccountid_value', 'accounts', config)
        if listedCustomers != -1:
                logging.info("-------------------------LIMIT ON CUSTOMERGROUP--------------------------")
                for customer in list(listedCustomers):
                    if customer['_parentaccountid_value'] is None:
                        pass
                    else:
                        parentCustomer=ro.getRecord(customer['_parentaccountid_value'],'accounts',config)
                        if parentCustomer['_vl_vevocsoport_partner_value'] ==group['vl_vevocsoportid']:
                            listedCustomers.remove(customer)
                limit_on_customer(listedCustomers,commandName,100000002,dailyDataVcs,config)
                if commandName=="daily" and not all(v == 0 for v in dailyDataVcs):
                    logging.info("Vevőcsoport neve: "+group['vl_name'])
                    logging.info("Eladott Kg: "+str(dailyDataVcs[0]))
                    logging.info("Eladott Összeg: "+str(dailyDataVcs[1]))
                    logging.info("Eladott Mennyiség: "+str(dailyDataVcs[2]))
                    logging.info("Limit Kg: "+str(dailyDataVcs[3]))
                    logging.info("Limit Összeg: "+str(dailyDataVcs[4]))
                    logging.info("Limit Mennyiség: "+str(dailyDataVcs[5]))
                    logging.info("Számlázandó összeg: "+str(dailyDataVcs[6]))
                    data = {
                        "vl_eladott_kg_vcs":dailyDataVcs[0],
                        "vl_eladott_osszeg_vcs":dailyDataVcs[1],
                        "vl_eladott_mennyiseg_vcs":dailyDataVcs[2],
                        "vl_limit_kg_vcs":dailyDataVcs[3],
                        "vl_limit_osszeg_vcs":dailyDataVcs[4],
                        "vl_limit_mennyiseg_vcs":dailyDataVcs[5],
                        "vl_vegosszeg_huf_vcs":str(dailyDataVcs[6]),
                        "vl_hatralevo_kg_vcs":dailyDataVcs[3]-dailyDataVcs[0],
                        "vl_hatralevo_osszeg_vcs":dailyDataVcs[4]-dailyDataVcs[1],
                        "vl_hatralevo_mennyiseg_vcs":dailyDataVcs[5]-dailyDataVcs[6]
                    }
                    ro.updateRecord(data,group['vl_vevocsoportid'],'vl_vevocsoports',config)

def limit_on_customer(listedCustomers,commandName,limitLevel,dailyDataVcs,config):
    for customer in listedCustomers:
        #[eladottKg,eladottOsszeg,eladottMennyiseg,limitKg,limitOsszeg,limitMennyiseg,vegosszeg]
        dailyData=[0,0,0,0,0,0,0]
        filt = {
            'filter1': {
                'field': '_vl_ugyfel_value',
                'operator': 'eq',
                'value': customer['accountid']
            }
        }
        listedContracts = ro.queryRecords(filt, '_vl_szamlazasi_partner_szerzodes_value,vl_szerzodes_statusza,vl_jovahagyas_statusza,vl_szerzodesszam,vl_limit_figyeles_szintje_szerz,_vl_ugyfel_value,vl_szamlazasi_periodus_kezdete_szerzodes,vl_szamlazasi_periodus_szerzodes,vl_szerzodes_kategoria,vl_szerzodes_lejarata', 'vl_szerzodeseks', config)
        if listedContracts != -1:
                logging.info("-------------------------LIMIT ON CUSTOMER--------------------------")
            #for contract in list(listedContracts):
                #if contract['vl_szerzodes_statusza']!=100000003 or contract['vl_jovahagyas_statusza']!=True:
                 #   listedContracts.remove(contract)
            #if listedContracts != -1:
                limit_on_contract(listedContracts,commandName,limitLevel,dailyData,config)
                dailyDataVcs[0]+=dailyData[0]
                dailyDataVcs[1]+=dailyData[1]
                dailyDataVcs[2]+=dailyData[2]
                dailyDataVcs[3]+=dailyData[3]
                dailyDataVcs[4]+=dailyData[4]
                dailyDataVcs[5]+=dailyData[5]
                dailyDataVcs[6]+=dailyData[6]
                if commandName=="daily" and limitLevel==100000001 and not all(v == 0 for v in dailyData):
                    logging.info("Partner neve: "+customer['name'])
                    logging.info("Eladott Kg: "+str(dailyData[0]))
                    logging.info("Eladott Összeg: "+str(dailyData[1]))
                    logging.info("Eladott Mennyiség: "+str(dailyData[2]))
                    logging.info("Limit Kg: "+str(dailyData[3]))
                    logging.info("Limit Összeg: "+str(dailyData[4]))
                    logging.info("Limit Mennyiség: "+str(dailyData[5]))
                    logging.info("Számlázandó összeg: "+str(dailyData[6]))
                    data = {
                        "vl_eladott_kg_partner":dailyData[0],
                        "vl_eladott_osszeg_partner":dailyData[1],
                        "vl_eladott_mennyiseg_partner":dailyData[2],
                        "vl_limit_kg_partner":dailyData[3],
                        "vl_limit_osszeg_partner":dailyData[4],
                        "vl_limit_mennyiseg_partner":dailyData[5],
                        "vl_vegosszeg_huf":str(dailyData[6]),
                        "vl_hatralevo_kg_partner":dailyData[3]-dailyData[0],
                        "vl_hatralevo_osszeg_partner":dailyData[4]-dailyData[1],
                        "vl_hatralevo_mennyiseg_partner":dailyData[5]-dailyData[6]
                    }
                    ro.updateRecord(data,customer['accountid'],'accounts',config)
        else: logging.info("Nincsenek szerződések a " + customer['name']+" nevű ügyfélhez")

def limit_on_contract(listedContracts,commandName,limitLevel,dailyData,config):
    logging.info("-------------------------LIMIT ON CONTRACT--------------------------")
    soldKgPartner=0
    soldHufPartner=0
    soldUnitPartner=0
    limitKgPartner=0
    limitHufPartner=0
    limitUnitPartner=0
    billPartner=0
    for contract in listedContracts:
        logging.info('!!!! Szerződés: '+str(contract['vl_szerzodesszam'])+' !!!!')
        #teljesítési periodus megadása
        if commandName=="monthly":
            last_day_of_prev_month = datetime.datetime.today().replace(day=1)-relativedelta(days=1)
            if contract['vl_szamlazasi_periodus_szerzodes'] == 100000000:
                first_day_of_the_period=(last_day_of_prev_month-relativedelta(months=0)).replace(day=1)-relativedelta(days=1)
                periodDays=(last_day_of_prev_month-first_day_of_the_period).days
                billRowCount=0
            elif contract['vl_szamlazasi_periodus_szerzodes'] == 100000001:
                first_day_of_the_period=(last_day_of_prev_month-relativedelta(months=2)).replace(day=1)-relativedelta(days=1)
                periodDays=(last_day_of_prev_month-first_day_of_the_period).days
                billRowCount=2       
            elif contract['vl_szamlazasi_periodus_szerzodes'] == 100000002:
                first_day_of_the_period=(last_day_of_prev_month-relativedelta(months=5)).replace(day=1)-relativedelta(days=1)
                periodDays=(last_day_of_prev_month-first_day_of_the_period).days
                billRowCount=5 
            elif contract['vl_szamlazasi_periodus_szerzodes'] == 100000003:
                first_day_of_the_period=(last_day_of_prev_month-relativedelta(months=11)).replace(day=1)-relativedelta(days=1)
                periodDays=(last_day_of_prev_month-first_day_of_the_period).days 
                billRowCount=11  
            else:
                logging.info('Nincs megadva periódus a szerződéhez')
                continue
        elif commandName=="daily":
            last_day_of_current_month=(datetime.datetime.today()+relativedelta(months=1)).replace(day=1)-relativedelta(days=1)
            if contract['vl_szamlazasi_periodus_szerzodes'] == 100000000:
                first_day_of_the_period=(last_day_of_current_month-relativedelta(months=0)).replace(day=1)-relativedelta(days=1)
                periodDays=(last_day_of_current_month-first_day_of_the_period).days
                billRowCount=0
            elif contract['vl_szamlazasi_periodus_szerzodes'] == 100000001:
                first_day_of_the_period=(last_day_of_current_month-relativedelta(months=2)).replace(day=1)-relativedelta(days=1)
                periodDays=(last_day_of_current_month-first_day_of_the_period).days
                billRowCount=2            
            elif contract['vl_szamlazasi_periodus_szerzodes'] == 100000002:
                first_day_of_the_period=(last_day_of_current_month-relativedelta(months=5)).replace(day=1)-relativedelta(days=1)
                periodDays=(last_day_of_current_month-first_day_of_the_period).days   
                billRowCount=5
            elif contract['vl_szamlazasi_periodus_szerzodes'] == 100000003:
                first_day_of_the_period=(last_day_of_current_month-relativedelta(months=11)).replace(day=1)-relativedelta(days=1)
                periodDays=(last_day_of_current_month-first_day_of_the_period).days  
                billRowCount=11 
            else:
                logging.info('Nincs megadva periódus a szerződéhez')
                continue
        logging.info('Periódus hossza: '+str(periodDays))
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
            contractProducts = ro.queryRecords(filt, '_vl_szerzodesek_limit_termek_value,vl_limitvizsgalatialap,_vl_kapcsolodo_termek_value,_vl_vallalt_termek_csoport_value', 'vl_limittermeksorais', config)
            contractProductsKg=[]
            contractProductsHuf=[]
            contractProductsUnit=[]
            if contractProducts == -1:
                logging.info('Nincs megadva a szerződéshez termék')
                continue
            for product in contractProducts:
                if product['vl_limitvizsgalatialap']==100000000:
                    if product['_vl_kapcsolodo_termek_value'] is None or product['_vl_kapcsolodo_termek_value']=="":
                        for prod in allProducts:
                            if product['_vl_vallalt_termek_csoport_value']==prod['_vl_folerendelt_termek_csoport_value']:
                                contractProductsKg.append(prod)
                    else:
                        contractProductsKg.append(ro.getRecord(product['_vl_kapcsolodo_termek_value'],'products',config))
                elif product['vl_limitvizsgalatialap']==100000001:
                    if product['_vl_kapcsolodo_termek_value'] is None or product['_vl_kapcsolodo_termek_value']=="":
                        for prod in allProducts:
                            if product['_vl_vallalt_termek_csoport_value']==prod['_vl_folerendelt_termek_csoport_value']:
                                contractProductsHuf.append(prod)                    
                    else:
                        contractProductsHuf.append(ro.getRecord(product['_vl_kapcsolodo_termek_value'],'products',config))
                else: 
                    if product['_vl_kapcsolodo_termek_value'] is None or product['_vl_kapcsolodo_termek_value']=="":
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
                        #limites sor
                        if contractLine['vl_limittel_erintett_szerzodessor']==True:
                            logging.info('Szerződés sor: '+contractLine['vl_name'])
                            pos=ro.getRecord(contractLine['_vl_kapcsolodo_pos_value'],'vl_poses',config)
                            posDictionary[contractLine['vl_name']]=pos['vl_pos_id']
                            logging.info('POS: '+str(pos['vl_pos_id']))
                            #Kg
                            if contractLine['vl_limitertek_alapja'] == 100000000:
                                soldKg+=AD_calculate(contractLine,contract,periodDays,bcHead,bcLine,pos,contractProductsKg,contractProductsHuf,contractProductsUnit)
                                limitKg+=float(contractLine['vl_limit_erteke'])
                                #if contractLine['vl_limit_koztes_ertek']:
                                #    halfLimitKg+=float(contractLine['vl_limit_koztes_ertek'])
                            #Huf
                            elif contractLine['vl_limitertek_alapja'] == 100000001:
                                soldHuf+=AD_calculate(contractLine,contract,periodDays,bcHead,bcLine,pos,contractProductsKg,contractProductsHuf,contractProductsUnit)
                                limitHuf+=float(contractLine['vl_limit_erteke'])
                                #if contractLine['vl_limit_koztes_ertek']:
                                #    halfLimitHuf+=float(contractLine['vl_limit_koztes_ertek'])
                            #Unit
                            elif contractLine['vl_limitertek_alapja'] == 100000002:
                                soldUnit+=AD_calculate(contractLine,contract,periodDays,bcHead,bcLine,pos,contractProductsKg,contractProductsHuf,contractProductsUnit)
                                limitUnit+=float(contractLine['vl_limit_erteke'])
                                #if contractLine['vl_limit_koztes_ertek']:
                                #    halfLimitUnit+=float(contractLine['vl_limit_koztes_ertek'])
        '''if halfLimitHuf:
            halfLimitHuf=limitHuf*(halfLimitHuf/100)
        if halfLimitKg:
            halfLimitKg=limitKg*(halfLimitKg/100)
        if halfLimitUnit:
            halfLimitUnit=limitUnit*(halfLimitUnit/100)'''
        if listedContractLines != -1:                    
            logging.info('Eladott kg:'+str(soldKg))
            soldKgPartner+=soldKg
            logging.info('Eladott összeg: ' + str(soldHuf))
            soldHufPartner+=soldHuf
            logging.info('Eladott mennyiség: '+str(soldUnit))
            soldUnitPartner+=soldUnit
            logging.info('Limit kg: '+str(limitKg))
            limitKgPartner+=limitKg
            logging.info('Limit összeg: '+str(limitHuf))
            limitHufPartner+=limitHuf
            logging.info('Limit mennyiség: '+str(limitUnit))
            limitUnitPartner+=limitUnit
            #logging.info('Köztes limit kg: '+str(halfLimitKg))
            #logging.info('Köztes limit összeg: '+str(halfLimitHuf))
            #logging.info('Köztes limit mennyiség: '+str(halfLimitUnit))
            id=datetime.datetime.today().timestamp()
            data = {
                #"regardingobjectid@odata.bind": "incidents({})".format(incidentid),
                "vl_eladott_kg":soldKg,
                "vl_eladott_mennyiseg":soldUnit,
                "vl_eladott_osszeg":soldHuf,
                #"vl_koztes_limit_kg":halfLimitKg,
                #"vl_koztes_limit_mennyiseg":halfLimitUnit,
                #"vl_koztes_limit_osszeg":halfLimitHuf,
                "vl_limit_kg":limitKg,
                "vl_limit_mennyiseg":limitUnit,
                "vl_limit_osszeg":limitHuf,
                "vl_periodus":periodDays,
                "vl_limit_szint":limitLevel,
                "vl_name":contract['vl_szerzodesszam'],
                #ideiglenes id mező
                "vl_korrekcio":id
            }
            if ro.createRecord(data,'vl_szolgaltatasszamlazases',config) > 400:
                logging.info("Nem sikerült a létrehozás!")
            else:
                filt = {
                    'filter1': {
                        'field': 'vl_limit_szint',
                        'operator': 'eq',
                        'value': str(limitLevel)
                    }
                }
                invoices=ro.queryRecords(filt,'vl_korrekcio,vl_name','vl_szolgaltatasszamlazases',config)
                for record in invoices:
                    if int(record['vl_korrekcio'])==int(id) and record['vl_name']==contract['vl_szerzodesszam']:
                        invoice=record
            for i in range(billRowCount+1):
                if soldKg >= limitKg and soldUnit >= limitUnit and soldHuf >= limitHuf:
                    for contractLine in listedContractLines:
                        logging.info(contractLine)
                        if contractLine['vl_limittel_erintett_szerzodessor'] is False:
                            data = {
                                "vl_vevo@odata.bind":"accounts({})".format(contract['_vl_szamlazasi_partner_szerzodes_value']),
                                "vl_name":contractLine['vl_name'],
                                "vl_szerzodes":contract['vl_szerzodesszam'],
                                "vl_szolgaltatas_szamlazas@odata.bind":"vl_szolgaltatasszamlazases({})".format(invoice['vl_szolgaltatasszamlazasid']),
                                "vl_dij_tipus":100000002,
                                "vl_szamlazando_osszeg":contractLine['vl_fix_berleti_dij'],
                                "vl_berl_uzem_dij_kedv":0,
                                "vl_berl_uzem_dij_kedv_eredeti":0
                            }
                            if commandName=="monthly":
                                if ro.createRecord(data,'vl_szolgaltatasszamlazassorais',config) > 400:
                                    logging.info("Nem sikerült a létrehozás!")
                            logging.info('A következő sor fix díjas:')
                            logging.info('Számlázandó szerződés:' + str(contract['vl_szerzodesszam']) + ' Számlázandó sor:' + str(contractLine['vl_name'])+ ' Összeg:' + str(int(contractLine['vl_fix_berleti_dij']))+"Ft")
                            billPartner+=int(contractLine['vl_fix_berleti_dij'])
                        else:
                            data = {
                                "vl_vevo@odata.bind":"accounts({})".format(contract['_vl_szamlazasi_partner_szerzodes_value']),
                                "vl_name":contractLine['vl_name'],
                                "vl_pos":posDictionary[contractLine['vl_name']],
                                "vl_szerzodes":contract['vl_szerzodesszam'],
                                "vl_szolgaltatas_szamlazas@odata.bind":"vl_szolgaltatasszamlazases({})".format(invoice['vl_szolgaltatasszamlazasid']),
                                "vl_dij_tipus":100000000,
                                "vl_szamlazando_osszeg":0,
                                "vl_berl_uzem_dij_kedv":100,
                                "vl_berl_uzem_dij_kedv_eredeti":100
                            }
                            if commandName=="monthly":
                                if ro.createRecord(data,'vl_szolgaltatasszamlazassorais',config) > 400:
                                    logging.info("Nem sikerült a létrehozás!")
                            logging.info('A következő sor elérte a limitet:')
                            logging.info('Számlázandó szerződés:' + str(contract['vl_szerzodesszam']) + ' Számlázandó sor:' + str(contractLine['vl_name'])+ ' Összeg: 0 Ft')
                else:
                    for contractLine in listedContractLines:
                        if contractLine['vl_limittel_erintett_szerzodessor'] is False:
                            data = {
                                "vl_vevo@odata.bind":"accounts({})".format(contract['_vl_szamlazasi_partner_szerzodes_value']),
                                "vl_name":contractLine['vl_name'],
                                "vl_szerzodes":contract['vl_szerzodesszam'],
                                "vl_szolgaltatas_szamlazas@odata.bind":"vl_szolgaltatasszamlazases({})".format(invoice['vl_szolgaltatasszamlazasid']),
                                "vl_dij_tipus":100000002,
                                "vl_szamlazando_osszeg":contractLine['vl_fix_berleti_dij'],
                                "vl_berl_uzem_dij_kedv":0,
                                "vl_berl_uzem_dij_kedv_eredeti":0
                            }
                            if commandName=="monthly":
                                if ro.createRecord(data,'vl_szolgaltatasszamlazassorais',config) > 400:
                                    logging.info("Nem sikerült a létrehozás!")
                            logging.info('A következő sor fix díjas:')
                            logging.info('Számlázandó szerződés:' + str(contract['vl_szerzodesszam']) + ' Számlázandó sor:' + str(contractLine['vl_name'])+ ' Összeg:' + str(int(contractLine['vl_fix_berleti_dij']))+"Ft")
                            billPartner+=int(contractLine['vl_fix_berleti_dij'])
                        elif contractLine['vl_limit_koztes_ertek']:
                            if soldKg >= float(contractLine['vl_limit_erteke'])*limitKg and soldUnit >= float(contractLine['vl_limit_erteke'])*limitUnit and soldHuf >= float(contractLine['vl_limit_erteke'])*limitHuf:
                                data = {
                                    "vl_vevo@odata.bind":"accounts({})".format(contract['_vl_szamlazasi_partner_szerzodes_value']),
                                    "vl_name":contractLine['vl_name'],
                                    "vl_pos":posDictionary[contractLine['vl_name']],
                                    "vl_szerzodes":contract['vl_szerzodesszam'],
                                    "vl_szolgaltatas_szamlazas@odata.bind":"vl_szolgaltatasszamlazases({})".format(invoice['vl_szolgaltatasszamlazasid']),
                                    "vl_dij_tipus":100000001,
                                    "vl_szamlazando_osszeg":contractLine['vl_koztes_limit_berleti_dija_huf'],
                                    "vl_berl_uzem_dij_kedv":int(100*float(contractLine['vl_limit_erteke'])),
                                    "vl_berl_uzem_dij_kedv_eredeti":int(100*float(contractLine['vl_limit_erteke']))
                                }
                                if commandName=="monthly":
                                    if ro.createRecord(data,'vl_szolgaltatasszamlazassorais',config) > 400:
                                        logging.info("Nem sikerült a létrehozás!")
                                logging.info('A következő sor elérte a köztes limitet:')
                                logging.info('Számlázandó szerződés:' + str(contract['vl_szerzodesszam']) + ' Számlázandó sor:' + str(contractLine['vl_name'])+' Összeg:' + str(int(contractLine['vl_koztes_limit_berleti_dija_huf']))+"Ft")
                                billPartner+=int(contractLine['vl_koztes_limit_berleti_dija_huf'])
                            else:
                                data = {
                                    "vl_vevo@odata.bind":"accounts({})".format(contract['_vl_szamlazasi_partner_szerzodes_value']),
                                    "vl_name":contractLine['vl_name'],
                                    "vl_pos":posDictionary[contractLine['vl_name']],
                                    "vl_szerzodes":contract['vl_szerzodesszam'],
                                    "vl_szolgaltatas_szamlazas@odata.bind":"vl_szolgaltatasszamlazases({})".format(invoice['vl_szolgaltatasszamlazasid']),
                                    "vl_dij_tipus":100000003,
                                    "vl_szamlazando_osszeg":contractLine['vl_teljes_berleti_dij_huf'],
                                    "vl_berl_uzem_dij_kedv":0,
                                    "vl_berl_uzem_dij_kedv_eredeti":0
                                }
                                if commandName=="monthly":
                                    if ro.createRecord(data,'vl_szolgaltatasszamlazassorais',config) > 400:
                                        logging.info("Nem sikerült a létrehozás!")
                                logging.info('A következő sor nem érte el egyik limitet sem:')
                                logging.info('Számlázandó szerződés:' + str(contract['vl_szerzodesszam']) + ' Számlázandó sor:' + str(contractLine['vl_name'])+ ' Összeg:' + str(int(contractLine['vl_teljes_berleti_dij_huf']))+"Ft")
                                billPartner+=int(contractLine['vl_teljes_berleti_dij_huf'])
                        else:
                            data = {
                                "vl_vevo@odata.bind":"accounts({})".format(contract['_vl_szamlazasi_partner_szerzodes_value']),
                                "vl_name":contractLine['vl_name'],
                                "vl_pos":posDictionary[contractLine['vl_name']],
                                "vl_szerzodes":contract['vl_szerzodesszam'],
                                "vl_szolgaltatas_szamlazas@odata.bind":"vl_szolgaltatasszamlazases({})".format(invoice['vl_szolgaltatasszamlazasid']),
                                "vl_dij_tipus":100000003,
                                "vl_szamlazando_osszeg":contractLine['vl_teljes_berleti_dij_huf'],
                                "vl_berl_uzem_dij_kedv":0,
                                "vl_berl_uzem_dij_kedv_eredeti":0
                            }
                            if commandName=="monthly":
                                if ro.createRecord(data,'vl_szolgaltatasszamlazassorais',config) > 400:
                                    logging.info("Nem sikerült a létrehozás!")
                            logging.info('A következő sor nem érte el egyik limitet sem:')
                            logging.info('Számlázandó szerződés:' + str(contract['vl_szerzodesszam']) + ' Számlázandó sor:' + str(contractLine['vl_name'])+ ' Összeg:' + str(int(contractLine['vl_teljes_berleti_dij_huf']))+"Ft")
                            billPartner+=int(contractLine['vl_teljes_berleti_dij_huf'])
            ''' elif soldKg >= halfLimitKg and soldUnit >= halfLimitUnit and soldHuf >= halfLimitHuf:
                for contractLine in listedContractLines:
                    if contractLine['vl_limittel_erintett_szerzodessor'] is False:
                        data = {
                            "vl_vevo@odata.bind":"accounts({})".format(contract['_vl_szamlazasi_partner_szerzodes_value']),
                            "vl_name":contractLine['vl_name'],
                            "vl_szerzodes":contract['vl_szerzodesszam'],
                            "vl_szolgaltatas_szamlazas@odata.bind":"vl_szolgaltatasszamlazases({})".format(invoice['vl_szolgaltatasszamlazasid']),
                            "vl_dij_tipus":100000002,
                            "vl_szamlazando_osszeg":contractLine['vl_fix_berleti_dij']
                        }
                        if ro.createRecord(data,'vl_szolgaltatasszamlazassorais',config) > 400:
                            logging.info("Nem sikerült a létrehozás!")
                        logging.info('A következő sor fix díjas:')
                        logging.info('Számlázandó szerződés:' + str(contract['vl_szerzodesszam']) + ' Számlázandó sor:' + str(contractLine['vl_name'])+ ' Összeg:' + str(int(contractLine['vl_fix_berleti_dij']))+"Ft")
                    else:
                        data = {
                            "vl_vevo@odata.bind":"accounts({})".format(contract['_vl_szamlazasi_partner_szerzodes_value']),
                            "vl_name":contractLine['vl_name'],
                            "vl_pos":posDictionary[contractLine['vl_name']],
                            "vl_szerzodes":contract['vl_szerzodesszam'],
                            "vl_szolgaltatas_szamlazas@odata.bind":"vl_szolgaltatasszamlazases({})".format(invoice['vl_szolgaltatasszamlazasid']),
                            "vl_dij_tipus":100000001,
                            "vl_szamlazando_osszeg":contractLine['vl_koztes_limit_berleti_dija_huf']
                        }
                        if ro.createRecord(data,'vl_szolgaltatasszamlazassorais',config) > 400:
                            logging.info("Nem sikerült a létrehozás!")
                        logging.info('A következő sor elérte a köztes limitet:')
                        logging.info('Számlázandó szerződés:' + str(contract['vl_szerzodesszam']) + ' Számlázandó sor:' + str(contractLine['vl_name'])+' Összeg:' + str(int(contractLine['vl_koztes_limit_berleti_dija_huf']))+"Ft")
            else:
                for contractLine in listedContractLines:
                    if contractLine['vl_limittel_erintett_szerzodessor'] is False:
                        data = {
                            "vl_vevo@odata.bind":"accounts({})".format(contract['_vl_szamlazasi_partner_szerzodes_value']),
                            "vl_name":contractLine['vl_name'],
                            "vl_szerzodes":contract['vl_szerzodesszam'],
                            "vl_szolgaltatas_szamlazas@odata.bind":"vl_szolgaltatasszamlazases({})".format(invoice['vl_szolgaltatasszamlazasid']),
                            "vl_dij_tipus":100000002,
                            "vl_szamlazando_osszeg":contractLine['vl_fix_berleti_dij']
                        }
                        if ro.createRecord(data,'vl_szolgaltatasszamlazassorais',config) > 400:
                            logging.info("Nem sikerült a létrehozás!")
                        logging.info('A következő sor fix díjas:')
                        logging.info('Számlázandó szerződés:' + str(contract['vl_szerzodesszam']) + ' Számlázandó sor:' + str(contractLine['vl_name'])+ ' Összeg:' + str(int(contractLine['vl_fix_berleti_dij']))+"Ft")
                    else:
                        data = {
                            "vl_vevo@odata.bind":"accounts({})".format(contract['_vl_szamlazasi_partner_szerzodes_value']),
                            "vl_name":contractLine['vl_name'],
                            "vl_pos":posDictionary[contractLine['vl_name']],
                            "vl_szerzodes":contract['vl_szerzodesszam'],
                            "vl_szolgaltatas_szamlazas@odata.bind":"vl_szolgaltatasszamlazases({})".format(invoice['vl_szolgaltatasszamlazasid']),
                            "vl_dij_tipus":100000003,
                            "vl_szamlazando_osszeg":contractLine['vl_teljes_berleti_dij_huf']
                        }
                        if ro.createRecord(data,'vl_szolgaltatasszamlazassorais',config) > 400:
                            logging.info("Nem sikerült a létrehozás!")
                        logging.info('A következő sor nem érte el egyik limitet sem:')
                        logging.info('Számlázandó szerződés:' + str(contract['vl_szerzodesszam']) + ' Számlázandó sor:' + str(contractLine['vl_name'])+ ' Összeg:' + str(int(contractLine['vl_teljes_berleti_dij_huf']))+"Ft")
            '''
        if commandName=="daily" and limitLevel==100000000:
            logging.info(contract['vl_szerzodesszam'])
            logging.info("Eladott Kg: "+str(dailyData[0]))
            logging.info("Eladott Összeg: "+str(dailyData[1]))
            logging.info("Eladott Mennyiség: "+str(dailyData[2]))
            logging.info("Limit Kg: "+str(dailyData[3]))
            logging.info("Limit Összeg: "+str(dailyData[4]))
            logging.info("Limit Mennyiség: "+str(dailyData[5]))
            logging.info("Számlázandó összeg: "+str(dailyData[6]))
            data = {
                "vl_eladott_kg_szerz":dailyData[0],
                "vl_eladott_osszeg_szerz":dailyData[1],
                "vl_eladott_mennyiseg_szerz":dailyData[2],
                "vl_limit_kg_szerz":dailyData[3],
                "vl_limit_osszeg_szerz":dailyData[4],
                "vl_limit_mennyiseg_szerz":dailyData[5],
                "vl_vegosszeg_huf":str(dailyData[6]),
                "vl_hatralevo_kg_szerz":dailyData[3]-dailyData[0],
                "vl_hatralevo_osszeg_szerz":dailyData[4]-dailyData[1],
                "vl_hatralevo_mennyiseg_szerz":dailyData[5]-dailyData[6]
            }
            ro.updateRecord(data,contract['vl_szerzodesekid'],'vl_szerzodeseks',config)
    dailyData[0]=soldKgPartner
    dailyData[1]=soldHufPartner
    dailyData[2]=soldUnitPartner
    dailyData[3]=limitKgPartner
    dailyData[4]=limitHufPartner
    dailyData[5]=limitUnitPartner
    dailyData[6]=billPartner
if __name__ == "__main__":
    main()