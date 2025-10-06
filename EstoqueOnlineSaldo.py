#Importando bibliotecas
import requests
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm
from time import sleep, time
import warnings
warnings.filterwarnings('ignore')
import concurrent.futures
import os



#Criando a função de coleta de dados
def estoque_online_saldo(sku):
    chave_id = 
    url = 'http://ws.livekpl.onclick.com.br/AbacosWSplataforma.asmx'
    envelope_soup = f"""
    <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <EstoqueOnLineSaldo xmlns="http://www.kplsolucoes.com.br/ABACOSWebService">
      <ChaveIdentificacao>{chave_id}</ChaveIdentificacao>
      <CodigosProduto>{sku}</CodigosProduto>
    </EstoqueOnLineSaldo>
  </soap:Body>
</soap:Envelope>"""

    headers = {'Content-Type': 'text/xml; charset=utf-8'}

    r = requests.post(url, data = envelope_soup, headers = headers, timeout=30)
    return r.text



#Lendo a base de produtos KPL
produtos = pd.read_parquet('\\'.join(os.getcwd().split('\\')[:3]) + r'\Vitrine Direta Dropbox\Vitrine Direta\17. Tech\Dashboard\Base de dados Dash\Dados atualizados\produtos kpl.gzip')



#Realizando a coleta em paralelo
lista_sku, lista_almoxarifado, lista_estoque_disp = [], [], []

def processar_sku(sku):
    resposta = BeautifulSoup(estoque_online_saldo(sku), 'xml')
    results = []
    
    for j in range(0, len(resposta.find_all('DadosEstoqueResultado'))):
        try:
            sku = resposta.find_all('DadosEstoqueResultado')[j].find('CodigoProduto').text.strip()
            estoque_disp = resposta.find_all('DadosEstoqueResultado')[j].find('SaldoDisponivel').text.strip()
            almoxarifado = resposta.find_all('DadosEstoqueResultado')[j].find('NomeEstoque').text.strip()
            results.append((sku, estoque_disp, almoxarifado))
        except:
            time.sleep(5)
            sku = resposta.find_all('DadosEstoqueResultado')[j].find('CodigoProduto').text.strip()
            estoque_disp = resposta.find_all('DadosEstoqueResultado')[j].find('SaldoDisponivel').text.strip()
            almoxarifado = resposta.find_all('DadosEstoqueResultado')[j].find('NomeEstoque').text.strip()
            results.append((sku, estoque_disp, almoxarifado))
            
    return results

#Criando a paralelização da coleta
with concurrent.futures.ThreadPoolExecutor() as executor:
    futures = [executor.submit(processar_sku, produtos.iloc[i]['CodigoProduto']) for i in range(0, len(produtos))]
    
    for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures)):
        try:
            result = future.result()
            for sku, estoque_disp, almoxarifado in result:
                lista_sku.append(sku)
                lista_estoque_disp.append(estoque_disp)
                lista_almoxarifado.append(almoxarifado)
        except Exception as e:
            print(f"An error occurred: {e}")
            
            
            
#Criando o DataFrame com os estoques atualizados
db_estoque = pd.DataFrame()

db_estoque['CodigoProduto'] = lista_sku
db_estoque['SaldoDisponivel'] = lista_estoque_disp
db_estoque['NomeAlmoxarifadoOrigem'] = lista_almoxarifado


#Coletando os estoques dos skus que não foram localizados inicialmente
print('\nColeta secundária de estoques')
for sku in tqdm(set(produtos.CodigoProduto.unique()) - set(db_estoque.CodigoProduto.unique())):
    estoque_sku = processar_sku(sku)
    if len(estoque_sku) > 0:
        for j in range(0, len(estoque_sku)):
            db_estoque.loc[len(db_estoque)] = estoque_sku[j]


#Tratando as colunas para salvar o arquivo
db_estoque.SKU = db_estoque.CodigoProduto.astype(str)
db_estoque.Estoque = db_estoque.SaldoDisponivel.astype(int)
db_estoque.Almoxarifado = db_estoque.NomeAlmoxarifadoOrigem.astype(str)
db_estoque = db_estoque[~db_estoque['NomeAlmoxarifadoOrigem'].isin(['FATURAMENTO VD RJ - COMUNICAÇÃO VISUAL', 'FATURAMENTO VD RJ - MANTA MAGNETICA CV','FATURAMENTO VD SP - MANTA MAGNETICA CV', 'FULFILLMENT AMAZON VD SP'])]


########### BLOCO QUE CÓDIGO PARA A CRIAÇÃO DO ESTOQUE DIÁRIO ###########
from datetime import datetime
import shutil
shutil.copy('\\'.join(os.getcwd().split('\\')[:3]) + r'\Vitrine Direta Dropbox\Vitrine Direta\17. Tech\Dashboard\Base de dados Dash\Dados atualizados\estoque_diario.gzip', '\\'.join(os.getcwd().split('\\')[:3]) + r'\Vitrine Direta Dropbox\Vitrine Direta\17. Tech\Dashboard\Base de dados Dash\Dados atualizados\Backup dados atualizados\estoque_diario.gzip')
estoque_diario = pd.read_parquet('\\'.join(os.getcwd().split('\\')[:3]) + r'\Vitrine Direta Dropbox\Vitrine Direta\17. Tech\Dashboard\Base de dados Dash\Dados atualizados\Backup dados atualizados\estoque_diario.gzip')
db_estoque['Data'] = datetime.date(datetime.today())
if datetime.date(datetime.fromtimestamp(os.path.getctime('\\'.join(os.getcwd().split('\\')[:3]) + r'\Vitrine Direta Dropbox\Vitrine Direta\17. Tech\Dashboard\Base de dados Dash\Dados atualizados\Backup dados atualizados\estoque_diario.gzip'))) == datetime.date(datetime.today()):
    pass
else:
    estoque_diario = pd.concat([estoque_diario, db_estoque], ignore_index = True)
    estoque_diario['aux'] = estoque_diario['CodigoProduto'] + estoque_diario['NomeAlmoxarifadoOrigem'] + estoque_diario.Data.astype(str)
    estoque_diario = estoque_diario.drop_duplicates(keep = 'first', subset = 'aux').reset_index(drop = True)
    estoque_diario.drop(columns = ['aux'], inplace = True)
    estoque_diario.to_parquet('\\'.join(os.getcwd().split('\\')[:3]) + r'\Vitrine Direta Dropbox\Vitrine Direta\17. Tech\Dashboard\Base de dados Dash\Dados atualizados\estoque_diario.gzip', index = False, compression = 'gzip')
db_estoque.drop(columns = ['Data'], inplace = True)
########### BLOCO QUE CÓDIGO PARA A CRIAÇÃO DO ESTOQUE DIÁRIO ###########



def preco_online(lista_skus):
    
    lista_produtos = []
    for produto in lista_skus:
        lista_produtos.append(f'<string>{produto}</string>')
    
    chave_id = '7C2E97AA-1BC0-4C1F-9340-EC565A4520E4'
    url = 'http://ws.livekpl.onclick.com.br/AbacosWSplataforma.asmx'
    envelope_soup = f"""
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <PrecoOnLine xmlns="http://www.kplsolucoes.com.br/ABACOSWebService">
      <ChaveIdentificacao>{chave_id}</ChaveIdentificacao>
      <ListaDeCodigosProdutos>
      {lista_produtos}
      </ListaDeCodigosProdutos>
    </PrecoOnLine>
  </soap:Body>
</soap:Envelope>"""

    headers = {'Content-Type': 'text/xml; charset=utf-8'}

    r = requests.post(url, data = envelope_soup, headers = headers, timeout=500)
    return BeautifulSoup(r.text, 'xml')



a = time()

#Coletando os preços atuais

print('\nColetando os preços dos SKUS...')

resposta = preco_online(list(produtos['CodigoProduto']))

lista_skus, lista_precos = [], []
for produto in resposta.find_all('DadosPrecoResultado'):
    lista_skus.append(produto.find('CodigoProduto').text)
    lista_precos.append(float(produto.find('PrecoTabela').text))
    
df_preco = pd.DataFrame()
df_preco['SKU'] = lista_skus
df_preco['Preço'] = lista_precos


#Salvando as informações de preços e custos no datafram
db_estoque['Custo'] = pd.merge(left = db_estoque, right = produtos, on = 'CodigoProduto', how = 'left')['CustoDoProduto']
db_estoque['Preço'] = pd.merge(left = db_estoque, right = df_preco, left_on = 'CodigoProduto', right_on = 'SKU', how = 'left')['Preço']

b = time()

print(f'Coleta de preços realizada em {str(round(b - a, 2))} seg')



print('\nFim!')
sleep(5)
#Salvando o arquivo parquet
db_estoque.to_parquet('\\'.join(os.getcwd().split('\\')[:3]) + r'\Vitrine Direta Dropbox\Vitrine Direta\17. Tech\Dashboard\Base de dados Dash\Dados atualizados\estoques_kpl.gzip', index = False, compression = 'gzip')
