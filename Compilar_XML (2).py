print('************************** COMPILADOR DE ARQUIVOS XML **************************')
caminho = input('\nDigite o caminho dos arquivos XML: \n')


import pandas as pd
from os import listdir
from bs4 import BeautifulSoup
from tqdm import tqdm
import numpy as np
from time import sleep


#Criando a função para processar multiplos skus
def processar_multiplo(soup, arquivo):
    lista_dict = {}
    lista_skus = []
    num_skus = len(soup.find_all('det'))

    lista_dict['Código NF'] = soup.find('infNFe').find('cNF').text
    lista_dict['Nota Fiscal'] = soup.find('infNFe').find('nNF').text
    lista_dict['Série'] = soup.find('infNFe').find('serie').text
    lista_dict['Data Emissão Nota'] = soup.find('infNFe').find('dhEmi').text
    lista_dict['Valor Frete'] = soup.find('vFrete').text
    #Se tiver número de pedido, coloca. Se não, deixa vazio
    try:
        lista_dict['Pedido'] = soup.find('xPed').text
    except:
        lista_dict['Pedido'] = np.nan

    #Processando multiplos skus
    for i in range(0, num_skus):
        dict_skus = {}
        dict_skus['SKU'] = soup.find_all('det')[i].find('cProd').text
        dict_skus['EAN'] = soup.find('infNFe').find('cEAN').text
        dict_skus['Nome produto'] = soup.find_all('det')[i].find('xProd').text
        dict_skus['Quantidade'] = soup.find_all('det')[i].find('qCom').text
        dict_skus['Valor Produto'] = soup.find_all('det')[i].find('vProd').text
        dict_skus['Base de calculo ICMS'] = soup.find_all('det')[i].find('vBC').text
        dict_skus['Aliquota ICMS'] = soup.find_all('det')[i].find('pICMS').text
        dict_skus['Valor ICMS'] = soup.find_all('det')[i].find('vICMS').text
        lista_skus.append(dict_skus)
        
    try:
        lista_dict['ICMS Destino'] = soup.find('vICMSUFDest').text
    except:
        lista_dict['ICMS Destino'] = np.nan
    lista_dict['ICMS Fecp'] = soup.find('vFCP').text
    try:
        lista_dict['Aliquota PIS'] = soup.find('pPIS').text
    except:
        lista_dict['Aliquota PIS'] = np.nan
    
    lista_dict['Valor PIS'] = soup.find('vPIS').text
    try:
        lista_dict['Aliquota COFINS'] = soup.find('pCOFINS').text
    except:
        lista_dict['Aliquota COFINS'] = np.nan
    lista_dict['Valor COFINS'] = soup.find('vCOFINS').text
    lista_dict['CFOP'] = soup.find('CFOP').text
    lista_dict['Natureza Operação'] = soup.find('natOp').text
    lista_dict['Arquivo'] = arquivo

    dataframe_export = pd.concat([pd.DataFrame(lista_skus), pd.DataFrame([lista_dict] * num_skus)], axis = 1)
    
    return dataframe_export


#Criando a função para processar apenas um sku
def processar_unico(soup, arquivo):
    lista_dict = {}
    lista_dict['Código NF'] = soup.find('infNFe').find('cNF').text
    lista_dict['Nota Fiscal'] = soup.find('infNFe').find('nNF').text
    lista_dict['Série'] = soup.find('infNFe').find('serie').text
    lista_dict['Data Emissão Nota'] = soup.find('infNFe').find('dhEmi').text
    #Se tiver número de pedido, coloca. Se não, deixa vazio
    try:
        lista_dict['Pedido'] = soup.find('xPed').text
    except:
        lista_dict['Pedido'] = np.nan
    lista_dict['SKU'] = soup.find('infNFe').find('cProd').text
    lista_dict['EAN'] = soup.find('infNFe').find('cEAN').text
    lista_dict['Nome produto'] = soup.find('infNFe').find('xProd').text
    lista_dict['Quantidade'] = soup.find('infNFe').find('prod').find('qCom').text
    lista_dict['Valor Produto'] = soup.find('infNFe').find('prod').find('vProd').text
    lista_dict['Valor Frete'] = soup.find('infNFe').find('vFrete').text
    lista_dict['Base de calculo ICMS'] = soup.find('vBC').text
    lista_dict['Aliquota ICMS'] = soup.find('pICMS').text
    lista_dict['Valor ICMS'] = soup.find('total').find('vICMS').text
    try:
        lista_dict['ICMS Destino'] = soup.find('vICMSUFDest').text
    except:
        lista_dict['ICMS Destino'] = np.nan
    lista_dict['ICMS Fecp'] = soup.find('vFCP').text
    try:
        lista_dict['Aliquota PIS'] = soup.find('pPIS').text
    except:
        lista_dict['Aliquota PIS'] = np.nan
    
    lista_dict['Valor PIS'] = soup.find('vPIS').text
    try:
        lista_dict['Aliquota COFINS'] = soup.find('pCOFINS').text
    except:
        lista_dict['Aliquota COFINS'] = np.nan
    lista_dict['Valor COFINS'] = soup.find('vCOFINS').text
    lista_dict['CFOP'] = soup.find('CFOP').text
    lista_dict['Natureza Operação'] = soup.find('natOp').text
    lista_dict['Arquivo'] = arquivo

    dataframe_export = pd.DataFrame([lista_dict])
    
    return dataframe_export


lista_dict = []
df_export = pd.DataFrame()

for arquivo in tqdm(listdir(caminho)):
    dicionario = {}
    aux = pd.DataFrame()
    
    if '.xml' in arquivo:
        xml = caminho + '\\' + arquivo
        with open(xml, 'r') as arq_xml:
            arq_xml = arq_xml.read()
        soup = BeautifulSoup(arq_xml, 'xml')
        
        tipo_arquivo = 'unico' if len(soup.find_all('det')) == 1 else 'multiplo'

        if tipo_arquivo == 'unico':
            df_aux = processar_unico(soup, arquivo)
            
        else:
            df_aux = processar_multiplo(soup, arquivo)

        df_export = pd.concat([df_export, df_aux], ignore_index = True)

for col in ['Valor Produto', 'Valor Frete', 'Base de calculo ICMS', 'Aliquota ICMS', 'Valor ICMS', 'ICMS Destino', 'ICMS Fecp', 'Aliquota PIS', 'Valor PIS', 'Aliquota COFINS', 'Valor COFINS', 'CFOP']:
    try:
        df_export[col] = df_export[col].astype(float)
    except:
        continue

df_export.to_excel(caminho + '\\Export XML.xlsx', index = False)


print('\nFIM!')
sleep(3)