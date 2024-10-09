#!/usr/bin/env python
# coding: utf-8

# In[41]:


print('******** ATUALIZAÇÃO INTELIPOST ********')


# In[42]:


import os
import pandas as pd
import shutil
import datetime
from multiprocessing import Pool
from tqdm import tqdm
import requests
import numpy as np
from multiprocessing import Pool
from concurrent.futures import ThreadPoolExecutor, as_completed
import json


# In[43]:


#Criando um backup do arquivo constante na página

shutil.copy(os.path.join(*os.getcwd().split('\\')[:3], 'Vitrine Direta Dropbox\Vitrine Direta', '17. Tech\Dashboard\Base de dados Dash\Dados atualizados').replace('C:', 'C:\\') + '\\intelipost.gzip', os.path.join(*os.getcwd().split('\\')[:3], 'Vitrine Direta Dropbox\Vitrine Direta', '17. Tech\Dashboard\Base de dados Dash\Dados atualizados\Backup dados atualizados').replace('C:', 'C:\\') + '\\intelipost.gzip')


# In[44]:


#Lendo o arquivo de Pedidos que consta no caminho
pedidos = pd.read_parquet(os.path.join(*os.getcwd().split('\\')[:3], 'Vitrine Direta Dropbox\Vitrine Direta', '17. Tech\Dashboard\Base de dados Dash\Dados atualizados').replace('C:', 'C:\\') + '\\pedidos.gzip')
pedidos['Pedido - Data movto.'] = pd.to_datetime(pedidos['Pedido - Data movto.'])

#Filtrando a relação de pedidos do arquivo
pedidos_nao_entregues = pedidos[(pedidos['Pedido - Status'] != 'ENTREGUE') & (pedidos['Cliente - grupo'] !='COMUNICAÇÃO VISUAL')].drop_duplicates().reset_index(drop = True)


# In[45]:


#Renomeando a coluna de pedidos
pedidos_nao_entregues.rename({'Pedido': 'Pedido - Nº'}, inplace = True, axis = 1)

#Filtrando apenas os pedidos dos últimos 120 dias
pedidos_nao_entregues = pedidos_nao_entregues[pedidos_nao_entregues['Pedido - Data movto.'] > (pd.to_datetime(datetime.date.today() - datetime.timedelta(days=120)))].reset_index(drop = True)


# In[46]:


#Criando a função para coletar os dados

def coletar_pedido(i):
    num_pedido = pedidos_nao_entregues.iloc[i]['Pedido - Nº']
    url = f"https://api.intelipost.com.br/api/v1/shipment_order/{num_pedido}"

    headers = {
        "api-key": "cef9a425a23e4a655c2a9a48392020a6032d77c472e8c95f15dac937a37ef153",
        "platform": "", "platform-version": "",
        "plugin": "", "plugin-version": "",
        "Content-Type": "", "Accept": "application/json"}

    response = requests.get(url, headers=headers)
    
    objeto_json = json.loads(response.text)
    
    if objeto_json['status'] == 'ERROR':
        
        lista_dict = {}
        
        lista_dict['Status'] = 'ERROR'
        lista_dict['Tipo do Erro'] = objeto_json['messages'][0]['text']
        lista_dict['Pedido'] = num_pedido
    
    else:
        lista_dict = {}
        
        lista_dict['Pedido'] = num_pedido
        lista_dict['tracking_url'] = objeto_json['content']['tracking_url']
        lista_dict['estimated_delivery_days_lp'] = objeto_json['content']['estimated_delivery_days_lp']
        lista_dict['origin_name'] = objeto_json['content']['origin_name']
        lista_dict['external_order_numbers'] = objeto_json['content']['external_order_numbers']['sales']
        lista_dict['logistic_provider_name'] = objeto_json['content']['logistic_provider_name']
        
        
        try:
            lista_dict['provider_shipping_cost'] = objeto_json['content']['provider_shipping_cost']
            lista_dict['additional_information'] = objeto_json['content']['additional_information']['key']
            lista_dict['shipped_date'] = objeto_json['content']['shipped_date']
            lista_dict['shipped_date_iso'] = objeto_json['content']['shipped_date_iso']
        except:
            lista_dict['provider_shipping_cost'] = np.nan
            lista_dict['shipped_date'] = np.nan
            lista_dict['shipped_date_iso'] = np.nan
            lista_dict['additional_information'] = np.nan
            
        lista_dict['sales_order_number'] = objeto_json['content']['sales_order_number']
        lista_dict['customer_shipping_costs'] = objeto_json['content']['customer_shipping_costs']
        lista_dict['markers'] = objeto_json['content']['markers']
        lista_dict['scheduled'] = objeto_json['content']['scheduled']
        lista_dict['estimated_delivery_date'] = objeto_json['content']['estimated_delivery_date']
        lista_dict['estimated_delivery_date_iso'] = objeto_json['content']['estimated_delivery_date_iso']
        lista_dict['origin_street'] = objeto_json['content']['origin_street']
        lista_dict['origin_number'] = objeto_json['content']['origin_number']
        lista_dict['origin_quarter'] = objeto_json['content']['origin_quarter']
        lista_dict['origin_city'] = objeto_json['content']['origin_city']
        lista_dict['origin_state_code'] = objeto_json['content']['origin_state_code']
        lista_dict['shipment_order_type'] = objeto_json['content']['shipment_order_type']
        lista_dict['warehouse_address_id'] = objeto_json['content']['warehouse_address_id']
        lista_dict['origin_zip_code'] = objeto_json['content']['origin_zip_code']
        lista_dict['origin_federal_tax_payer_id'] = objeto_json['content']['origin_federal_tax_payer_id']
        lista_dict['sales_channel'] = objeto_json['content']['sales_channel']
        lista_dict['attachments'] = objeto_json['content']['attachments']
        lista_dict['modified'] = objeto_json['content']['modified']
        lista_dict['modified_iso'] = objeto_json['content']['modified_iso']
        lista_dict['delivery_method_id'] = objeto_json['content']['delivery_method_id']
        lista_dict['end_customer'] = objeto_json['content']['end_customer']
        lista_dict['order_number'] = objeto_json['content']['order_number']
        # lista_dict['platform'] = objeto_json['content']['platform']
        lista_dict['delivery_method_name'] = objeto_json['content']['delivery_method_name']
        lista_dict['created'] = objeto_json['content']['created']
        lista_dict['created_iso'] = objeto_json['content']['created_iso']
        lista_dict['id'] = objeto_json['content']['id']
        
        try:
            lista_dict['invoice_key'] = objeto_json['content']['invoice_key']
        except:
            lista_dict['invoice_key'] = np.nan
            
        try:
            lista_dict['shipment_order_volume_array'] = objeto_json['content']['shipment_order_volume_array'][0]['shipment_order_volume_state_history_array'][0]
            lista_dict['shipment_order_volume_state_history_array'] = objeto_json['content']['shipment_order_volume_array'][0]['shipment_order_volume_state_history_array'][0]
        except:
            lista_dict['shipment_order_volume_array'] = np.nan
            lista_dict['shipment_order_volume_state_history_array'] = np.nan
        
        lista_dict['Cliente - first_name'] = objeto_json['content']['end_customer']['first_name']
        lista_dict['Cliente - last_name'] = objeto_json['content']['end_customer']['last_name']
        lista_dict['Cliente - email'] = objeto_json['content']['end_customer']['email']
        lista_dict['Cliente - phone'] = objeto_json['content']['end_customer']['phone']
        lista_dict['Cliente - cellphone'] = objeto_json['content']['end_customer']['cellphone']
        lista_dict['Cliente - is_company'] = objeto_json['content']['end_customer']['is_company']
        lista_dict['Cliente - federal_tax_payer_id'] = objeto_json['content']['end_customer']['federal_tax_payer_id']
        lista_dict['Cliente - state_tax_payer_id'] = objeto_json['content']['end_customer']['state_tax_payer_id']
        lista_dict['Cliente - shipping_address'] = objeto_json['content']['end_customer']['shipping_address']
        lista_dict['Cliente - shipping_number'] = objeto_json['content']['end_customer']['shipping_number']
        lista_dict['Cliente - shipping_additional'] = objeto_json['content']['end_customer']['shipping_additional']
        lista_dict['Cliente - shipping_reference'] = objeto_json['content']['end_customer']['shipping_reference']
        lista_dict['Cliente - shipping_quarter'] = objeto_json['content']['end_customer']['shipping_quarter']
        lista_dict['Cliente - shipping_city'] = objeto_json['content']['end_customer']['shipping_city']
        lista_dict['Cliente - shipping_zip_code'] = objeto_json['content']['end_customer']['shipping_zip_code']
        lista_dict['Cliente - shipping_state'] = objeto_json['content']['end_customer']['shipping_state']
        lista_dict['Cliente - shipping_state_code'] = objeto_json['content']['end_customer']['shipping_state_code']
        lista_dict['Cliente - shipping_country'] = objeto_json['content']['end_customer']['shipping_country']
        lista_dict['Cliente - notify'] = objeto_json['content']['end_customer']['notify']
        
        lista_dict['shipment_order_volume_number'] = objeto_json['content']['shipment_order_volume_array'][0]['shipment_order_volume_number']
        lista_dict['shipment_order_id'] = objeto_json['content']['shipment_order_volume_array'][0]['shipment_order_id']
        lista_dict['shipment_order_volume_state'] = objeto_json['content']['shipment_order_volume_array'][0]['shipment_order_volume_state']
        lista_dict['volume_type_code'] = objeto_json['content']['shipment_order_volume_array'][0]['volume_type_code']
        lista_dict['weight'] = objeto_json['content']['shipment_order_volume_array'][0]['weight']
        lista_dict['width'] = objeto_json['content']['shipment_order_volume_array'][0]['width']
        lista_dict['height'] = objeto_json['content']['shipment_order_volume_array'][0]['height']
        lista_dict['length'] = objeto_json['content']['shipment_order_volume_array'][0]['length']
        lista_dict['products_nature'] = objeto_json['content']['shipment_order_volume_array'][0]['products_nature']
        
        lista_dict['products_quantity'] = objeto_json['content']['shipment_order_volume_array'][0]['products_quantity']
        lista_dict['is_icms_exempt'] = objeto_json['content']['shipment_order_volume_array'][0]['is_icms_exempt']
        lista_dict['logistic_provider_tracking_code'] = objeto_json['content']['shipment_order_volume_array'][0]['logistic_provider_tracking_code']
        lista_dict['shipment_order_volume_number'] = objeto_json['content']['shipment_order_volume_array'][0]['shipment_order_volume_invoice']['shipment_order_volume_number']
        lista_dict['shipment_order_volume_id'] = objeto_json['content']['shipment_order_volume_array'][0]['shipment_order_volume_invoice']['shipment_order_volume_id']
        lista_dict['invoice_series'] = objeto_json['content']['shipment_order_volume_array'][0]['shipment_order_volume_invoice']['invoice_series']
        lista_dict['invoice_number'] = objeto_json['content']['shipment_order_volume_array'][0]['shipment_order_volume_invoice']['invoice_number']
        lista_dict['invoice_key'] = objeto_json['content']['shipment_order_volume_array'][0]['shipment_order_volume_invoice']['invoice_key']
        lista_dict['invoice_total_value'] = objeto_json['content']['shipment_order_volume_array'][0]['shipment_order_volume_invoice']['invoice_total_value']
        lista_dict['invoice_products_value'] = objeto_json['content']['shipment_order_volume_array'][0]['shipment_order_volume_invoice']['invoice_products_value']
        lista_dict['invoice_cfop'] = objeto_json['content']['shipment_order_volume_array'][0]['shipment_order_volume_invoice']['invoice_cfop']
        lista_dict['invoice_date_iso'] = objeto_json['content']['shipment_order_volume_array'][0]['shipment_order_volume_invoice']['invoice_date_iso']
        lista_dict['invoice_date_iso_iso'] = objeto_json['content']['shipment_order_volume_array'][0]['shipment_order_volume_invoice']['invoice_date_iso_iso']
        
        lista_dict['name'] = objeto_json['content']['shipment_order_volume_array'][0]['name']
        lista_dict['created'] = objeto_json['content']['shipment_order_volume_array'][0]['created']
        lista_dict['created_iso'] = objeto_json['content']['shipment_order_volume_array'][0]['created_iso']
        lista_dict['modified'] = objeto_json['content']['shipment_order_volume_array'][0]['modified']
        lista_dict['modified_iso'] = objeto_json['content']['shipment_order_volume_array'][0]['modified_iso']
        lista_dict['logistic_provider_label_hash'] = objeto_json['content']['shipment_order_volume_array'][0]['logistic_provider_label_hash']
        lista_dict['packaging_code'] = objeto_json['content']['shipment_order_volume_array'][0]['packaging_code']
        lista_dict['has_clarify_delivery_fail'] = objeto_json['content']['shipment_order_volume_array'][0]['has_clarify_delivery_fail']
        lista_dict['delivered'] = objeto_json['content']['shipment_order_volume_array'][0]['delivered']
        lista_dict['delivered_late'] = objeto_json['content']['shipment_order_volume_array'][0]['delivered_late']
        
        lista_dict['delivered_late_lp'] = objeto_json['content']['shipment_order_volume_array'][0]['delivered_late_lp']
        lista_dict['estimated_delivery_date'] = objeto_json['content']['shipment_order_volume_array'][0]['estimated_delivery_date']
        lista_dict['estimated_delivery_date_iso'] = objeto_json['content']['shipment_order_volume_array'][0]['estimated_delivery_date_iso']
        lista_dict['estimated_delivery_date_lp'] = objeto_json['content']['shipment_order_volume_array'][0]['estimated_delivery_date_lp']
        
        try:
            lista_dict['estimated_delivery_date_iso'] = objeto_json['content']['estimated_delivery_date_iso']
            lista_dict['estimated_delivery_date'] = objeto_json['content']['estimated_delivery_date']
            lista_dict['shipped_date_iso'] = objeto_json['content']['shipped_date_iso']
        except:
            lista_dict['estimated_delivery_date_iso'] = np.nan
            lista_dict['estimated_delivery_date'] = np.nan
            lista_dict['shipped_date_iso'] = np.nan
            
        lista_dict['original_estimated_delivery_date'] = objeto_json['content']['shipment_order_volume_array'][0]['original_estimated_delivery_date']
        lista_dict['original_estimated_delivery_date_iso'] = objeto_json['content']['shipment_order_volume_array'][0]['original_estimated_delivery_date_iso']
        lista_dict['original_estimated_delivery_date_lp'] = objeto_json['content']['shipment_order_volume_array'][0]['original_estimated_delivery_date_lp']
        lista_dict['estimated_delivery_days_lp'] = objeto_json['content']['shipment_order_volume_array'][0]['estimated_delivery_days_lp']
        lista_dict['pre_shipment_list_id'] = objeto_json['content']['shipment_order_volume_array'][0]['pre_shipment_list_id']
        lista_dict['logistic_provider_pre_shipment_list_id'] = objeto_json['content']['shipment_order_volume_array'][0]['logistic_provider_pre_shipment_list_id']
        lista_dict['client_pre_shipment_list'] = objeto_json['content']['shipment_order_volume_array'][0]['client_pre_shipment_list']
        lista_dict['pre_shipment_list_state'] = objeto_json['content']['shipment_order_volume_array'][0]['pre_shipment_list_state']
        lista_dict['attachments'] = objeto_json['content']['shipment_order_volume_array'][0]['attachments']
        lista_dict['logistics_provider_data'] = objeto_json['content']['shipment_order_volume_array'][0]['logistics_provider_data']
        lista_dict['content_declaration'] = objeto_json['content']['shipment_order_volume_array'][0]['content_declaration']
        lista_dict['shipment_order_volume_state_localized'] = objeto_json['content']['shipment_order_volume_array'][0]['shipment_order_volume_state_localized']
        lista_dict['delivered_date'] = objeto_json['content']['shipment_order_volume_array'][0]['delivered_date']
        lista_dict['shipped_date'] = objeto_json['content']['shipment_order_volume_array'][0]['shipped_date']
        lista_dict['shipment_order_volume_id'] = objeto_json['content']['shipment_order_volume_array'][0]['shipment_order_volume_id']
        lista_dict['tracking_code'] = objeto_json['content']['shipment_order_volume_array'][0]['tracking_code']
        
        lista_dict['shipment_order_volume_state_history_array - shipment_order_volume_id'] = objeto_json['content']['shipment_order_volume_array'][0]['shipment_order_volume_state_history_array'][0]['shipment_order_volume_id']
        lista_dict['shipment_order_volume_state_history_array - shipment_order_volume_state'] = objeto_json['content']['shipment_order_volume_array'][0]['shipment_order_volume_state_history_array'][0]['shipment_order_volume_state']
        lista_dict['shipment_order_volume_state_history_array - tracking_state'] = objeto_json['content']['shipment_order_volume_array'][0]['shipment_order_volume_state_history_array'][0]['tracking_state']
        lista_dict['shipment_order_volume_state_history_array - created'] = objeto_json['content']['shipment_order_volume_array'][0]['shipment_order_volume_state_history_array'][0]['created']
        lista_dict['shipment_order_volume_state_history_array - created_iso'] = objeto_json['content']['shipment_order_volume_array'][0]['shipment_order_volume_state_history_array'][0]['created_iso']
        lista_dict['shipment_order_volume_state_history_array - provider_message'] = objeto_json['content']['shipment_order_volume_array'][0]['shipment_order_volume_state_history_array'][0]['provider_message']
        lista_dict['shipment_order_volume_state_history_array - provider_state'] = objeto_json['content']['shipment_order_volume_array'][0]['shipment_order_volume_state_history_array'][0]['provider_state']
        lista_dict['shipment_order_volume_state_history_array - esprinter_message'] = objeto_json['content']['shipment_order_volume_array'][0]['shipment_order_volume_state_history_array'][0]['esprinter_message']
        lista_dict['shipment_order_volume_state_history_array - shipper_provider_state'] = objeto_json['content']['shipment_order_volume_array'][0]['shipment_order_volume_state_history_array'][0]['shipper_provider_state']
        
        lista_dict['shipment_order_volume_state_history_array - id'] = objeto_json['content']['shipment_order_volume_array'][0]['shipment_order_volume_state_history_array'][0]['shipment_volume_micro_state']['id']
        lista_dict['shipment_order_volume_state_history_array - code'] = objeto_json['content']['shipment_order_volume_array'][0]['shipment_order_volume_state_history_array'][0]['shipment_volume_micro_state']['code']
        lista_dict['shipment_order_volume_state_history_array - default_name'] = objeto_json['content']['shipment_order_volume_array'][0]['shipment_order_volume_state_history_array'][0]['shipment_volume_micro_state']['default_name']
        lista_dict['shipment_order_volume_state_history_array - i18n_name'] = objeto_json['content']['shipment_order_volume_array'][0]['shipment_order_volume_state_history_array'][0]['shipment_volume_micro_state']['i18n_name']
        lista_dict['shipment_order_volume_state_history_array - description'] = objeto_json['content']['shipment_order_volume_array'][0]['shipment_order_volume_state_history_array'][0]['shipment_volume_micro_state']['description']
        lista_dict['shipment_order_volume_state_history_array - shipment_order_volume_state_id'] = objeto_json['content']['shipment_order_volume_array'][0]['shipment_order_volume_state_history_array'][0]['shipment_volume_micro_state']['shipment_order_volume_state_id']
        lista_dict['shipment_order_volume_state_history_array - shipment_volume_state_source_id'] = objeto_json['content']['shipment_order_volume_array'][0]['shipment_order_volume_state_history_array'][0]['shipment_volume_micro_state']['shipment_volume_state_source_id']
        lista_dict['shipment_order_volume_state_history_array - shipment_volume_state_localized'] = objeto_json['content']['shipment_order_volume_array'][0]['shipment_order_volume_state_history_array'][0]['shipment_volume_micro_state']['shipment_volume_state_localized']
        lista_dict['shipment_order_volume_state_history_array - shipment_volume_state'] = objeto_json['content']['shipment_order_volume_array'][0]['shipment_order_volume_state_history_array'][0]['shipment_volume_micro_state']['shipment_volume_state']
        lista_dict['shipment_order_volume_state_history_array - name'] = objeto_json['content']['shipment_order_volume_array'][0]['shipment_order_volume_state_history_array'][0]['shipment_volume_micro_state']['name']
        
        lista_dict['shipment_order_volume_state_history_array - address'] = objeto_json['content']['shipment_order_volume_array'][0]['shipment_order_volume_state_history_array'][0]['location']['address']
        lista_dict['shipment_order_volume_state_history_array - number'] = objeto_json['content']['shipment_order_volume_array'][0]['shipment_order_volume_state_history_array'][0]['location']['number']
        lista_dict['shipment_order_volume_state_history_array - additional'] = objeto_json['content']['shipment_order_volume_array'][0]['shipment_order_volume_state_history_array'][0]['location']['additional']
        lista_dict['shipment_order_volume_state_history_array - reference'] = objeto_json['content']['shipment_order_volume_array'][0]['shipment_order_volume_state_history_array'][0]['location']['reference']
        lista_dict['shipment_order_volume_state_history_array - city'] = objeto_json['content']['shipment_order_volume_array'][0]['shipment_order_volume_state_history_array'][0]['location']['city']
        lista_dict['shipment_order_volume_state_history_array - state_code'] = objeto_json['content']['shipment_order_volume_array'][0]['shipment_order_volume_state_history_array'][0]['location']['state_code']
        lista_dict['shipment_order_volume_state_history_array - quarter'] = objeto_json['content']['shipment_order_volume_array'][0]['shipment_order_volume_state_history_array'][0]['location']['quarter']
        lista_dict['shipment_order_volume_state_history_array - zip_code'] = objeto_json['content']['shipment_order_volume_array'][0]['shipment_order_volume_state_history_array'][0]['location']['zip_code']
        lista_dict['shipment_order_volume_state_history_array - description'] = objeto_json['content']['shipment_order_volume_array'][0]['shipment_order_volume_state_history_array'][0]['location']['description']
        lista_dict['shipment_order_volume_state_history_array - latitude'] = objeto_json['content']['shipment_order_volume_array'][0]['shipment_order_volume_state_history_array'][0]['location']['latitude']
        lista_dict['shipment_order_volume_state_history_array - longitude'] = objeto_json['content']['shipment_order_volume_array'][0]['shipment_order_volume_state_history_array'][0]['location']['longitude']
        
        lista_dict['shipment_order_volume_state_history_array - request_hash'] = objeto_json['content']['shipment_order_volume_array'][0]['shipment_order_volume_state_history_array'][0]['request_hash']
        lista_dict['shipment_order_volume_state_history_array - request_origin'] = objeto_json['content']['shipment_order_volume_array'][0]['shipment_order_volume_state_history_array'][0]['request_origin']
        lista_dict['shipment_order_volume_state_history_array - attachments'] = objeto_json['content']['shipment_order_volume_array'][0]['shipment_order_volume_state_history_array'][0]['attachments']
        lista_dict['shipment_order_volume_state_history_array - shipment_order_volume_state_localized'] = objeto_json['content']['shipment_order_volume_array'][0]['shipment_order_volume_state_history_array'][0]['shipment_order_volume_state_localized']
        lista_dict['shipment_order_volume_state_history_array - shipment_order_volume_state_history'] = objeto_json['content']['shipment_order_volume_array'][0]['shipment_order_volume_state_history_array'][0]['shipment_order_volume_state_history']
        lista_dict['shipment_order_volume_state_history_array - event_date'] = objeto_json['content']['shipment_order_volume_array'][0]['shipment_order_volume_state_history_array'][0]['event_date']
        lista_dict['shipment_order_volume_state_history_array - event_date_iso'] = objeto_json['content']['shipment_order_volume_array'][0]['shipment_order_volume_state_history_array'][0]['event_date_iso']
        
        if objeto_json['content']['shipment_order_volume_array'][0]['shipment_order_volume_state_history_array'][0]['provider_state'] == None:
            lista_dict['Fonte'] = 'Embarcador'
        else:
            lista_dict['Fonte'] = 'Transportadora'
            
    return lista_dict


# In[ ]:


#Realizando a coleta dos dados

lista = []

with ThreadPoolExecutor(max_workers = int(os.cpu_count())) as executor:
    
    futures = [executor.submit(coletar_pedido, i) for i in range(len(pedidos_nao_entregues))]
    
    for future in tqdm(as_completed(futures), total=len(pedidos_nao_entregues)):
        lista.append(future.result())


# In[ ]:


#Criando o dataframe e inputando a data de coleta
coleta_intelipost = pd.DataFrame(lista)

coleta_intelipost['Coleta em'] = datetime.datetime.today()

#Criando coluna 'plataforma' com valor padronizado.
coleta_intelipost['platform'] = 'omnione'

traduzir = {'Pedido': 'Número Pedido', 'tracking_url': 'URL Rastreio', 'external_order_numbers': 'Número Pedido Intelipost', 'logistic_provider_name': 'Transportadora', 
'additional_information': 'Sub Canal', 'shipped_date_iso': 'Data de Envio', 'estimated_delivery_date': 'Data Estimada de Entrega', 
'invoice_number': 'Número Nota Fiscal', 'invoice_date_iso': 'Data Nota Fiscal', 'shipment_order_volume_state_localized': 'Status volume', 
'delivered_date': 'Data Entrega', 'shipment_order_volume_state_history_array - esprinter_message': 'Mensagem Impressa do Volume', 
'shipment_order_volume_state_history_array - shipment_order_volume_state_localized': 'Status localizado do Volume',
'modified_iso': 'Modificado em', 'tracking_code': 'Código Rastreio', 'invoice_key': 'DANFE', 'Fonte': 'Fonte'}


#Selecionando apenas as colunas que irão para nosso BI
coleta_intelipost = coleta_intelipost[traduzir.keys()].rename(traduzir, axis = 1)


#Corrigindo as colunas de datas
for i in range(0, len(coleta_intelipost)):
    try:
        coleta_intelipost.loc[i, 'Data de Envio'] = pd.to_datetime(coleta_intelipost.iloc[i]['Data de Envio']).tz_localize(None)
        coleta_intelipost.loc[i, 'Modificado em'] = pd.to_datetime(coleta_intelipost.iloc[i]['Modificado em']).tz_localize(None)
    except:
        continue


def converter_timestamps(df, coluna):
    lista_datas = []
    for i in range(0, len(df)):
        if pd.isna(df.iloc[i][coluna]):
            lista_datas.append(np.nan)
        else:
            lista_datas.append(datetime.datetime.fromtimestamp(df.iloc[i][coluna] / 1000))
    df[coluna] = lista_datas
    
converter_timestamps(coleta_intelipost, 'Data Nota Fiscal')
converter_timestamps(coleta_intelipost, 'Data Estimada de Entrega')
converter_timestamps(coleta_intelipost, 'Data Entrega')


coleta_intelipost = coleta_intelipost.drop_duplicates().reset_index(drop = True)



# In[ ]:


#Salvando a tabela na rede
coleta_intelipost.to_parquet(os.path.join(*os.getcwd().split('\\')[:3], 'Vitrine Direta Dropbox\Vitrine Direta', '17. Tech\Dashboard\Base de dados Dash\Dados atualizados').replace('C:', 'C:\\') + '\\intelipost.gzip', index = False, compression = 'gzip')


from time import sleep
print('\nFIM!')
sleep(5)


# In[ ]:




