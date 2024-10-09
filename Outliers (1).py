def disparar_email(html_formatado, semana):
    import smtplib
    from datetime import datetime
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    from email.mime.base import MIMEBase
    from email import encoders
    import os
    
    #Dados de acesso ao servidor e enviador
    senha_app = 'jabjrqyjucgohfwr'
    email_enviador = 'eliseu.santos.vitrinedireta@gmail.com'
    servidor_smtp = 'smtp.gmail.com'
    porta = 587
    # recebedor_email = ['eliseu.santos@vitrinedireta.com.br', 'pedro.seixas@vitrinedireta.com.br', 'ademar.nascimento@vitrinedireta.com.br', 'pedro.cesar@vitrinedireta.com.br', 'bruno@geralfilmes.com.br']
    recebedor_email = ['eliseu.santos@vitrinedireta.com.br']
    assunto = f'Outliers - {datetime.date(datetime.today())} - Semana {semana_num}'

    #Criando a mensagem no formato mime
    mensagem = MIMEMultipart()
    mensagem['From'] = email_enviador
    mensagem['To'] = ', '.join(recebedor_email)
    mensagem['Subject'] = assunto
    mensagem.attach(MIMEText(html_formatado, 'html'))

    '''ADAPTAÇÃO PARA INCLUIR O DATAFRAME'''
    # 1. Gerar o DataFrame e salvar como CSV
    arquivo_excel = 'Outliers.xlsx'

    # 2. Anexar o arquivo CSV
    with open(arquivo_excel, 'rb') as attachment:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header(
            'Content-Disposition',
            f'attachment; filename= {os.path.basename(arquivo_excel)}',
        )
        mensagem.attach(part)
    '''ADAPTAÇÃO PARA INCLUIR O DATAFRAME'''

    #Fazendo o disparo propriamente
    try:
        servidor = smtplib.SMTP(servidor_smtp, porta)
        servidor.starttls()
        servidor.login(email_enviador, senha_app)
        servidor.sendmail(email_enviador, recebedor_email, mensagem.as_string())
        print("E-mail enviado com sucesso")
        return "E-mail enviado com sucesso"
    except Exception as e:
        print(f"Houve algum erro: {e}")
        return e
    finally:
        servidor.quit()


import pandas as pd
import datetime
import os
import numpy as np
fator_outlier = 1.5

caminho = '\\'.join(os.getcwd().split('\\')[:3]) + r'\Vitrine Direta Dropbox\Vitrine Direta\17. Tech\Dashboard\Base de dados Dash\Dados atualizados'


itens_pedidos = pd.read_parquet(caminho + '\\itens pedidos.gzip')
skus_bbtudo = pd.read_excel(caminho + '\Backup dados atualizados\SKUs BBTUDO.xlsx')
times = pd.read_excel(caminho + '\Backup dados atualizados\Times.xlsx')


itens_pedidos = itens_pedidos[~itens_pedidos['Item - Família'].str.contains('COMUNICAÇÃO')]
itens_pedidos = itens_pedidos[~itens_pedidos['Item - Classificação'].str.contains('COMUNICAÇÃO')]
itens_pedidos = itens_pedidos[~itens_pedidos['Representante - Setor de venda'].str.contains('COMUNICAÇÃO')].reset_index(drop = True)
itens_pedidos['Time'] = pd.merge(left = itens_pedidos, right = times, left_on = 'Item - Classificação', right_on = 'Fornecedor', how = 'left')['Time'].fillna('Puericultura')


#Filtrando pedidos acontecidos nos últimos 80 dias
itens_pedidos = itens_pedidos[pd.to_datetime(itens_pedidos['Pedido - Dat. de movto.']) > (pd.Timestamp.now() - pd.Timedelta(days=150))].reset_index(drop = True)



#Criando a coluna de semana
itens_pedidos['Semana'] = pd.to_datetime(itens_pedidos['Pedido - Dat. de movto.']).dt.isocalendar().week

#Filtrando apenas as ultimas 12 semanas
itens_pedidos = itens_pedidos[itens_pedidos.Semana > itens_pedidos.Semana.max() - 14].reset_index(drop = True)



#QUANTIDADE
pedidos_pivot = itens_pedidos.pivot_table(index = ['Item - Código do produto', 'Time'], columns = 'Semana', values = 'Item - Qtde. pedida', aggfunc = 'sum').reset_index()

pedidos_pivot['Q1'], pedidos_pivot['Q3'], pedidos_pivot['Corte'] = np.nan, np.nan, np.nan

#Fazendo calculo de IIQ e Corte
for i in range(0, len(pedidos_pivot)):
    pedidos_pivot.loc[i, 'Q1'] = pedidos_pivot.iloc[i][2:-2].quantile(0.25)
    pedidos_pivot.loc[i, 'Q3'] = pedidos_pivot.iloc[i][2:-2].quantile(0.75)
    pedidos_pivot.loc[i, 'IIQ'] = pedidos_pivot.iloc[i][2:-2].quantile(0.75) - pedidos_pivot.iloc[i][2:-2].quantile(0.25)
    pedidos_pivot.loc[i, 'Corte'] = pedidos_pivot.iloc[i][2:-2].quantile(0.75) + ((pedidos_pivot.iloc[i][2:-2].quantile(0.75) - pedidos_pivot.iloc[i][2:-2].quantile(0.25)) * fator_outlier)

pedidos_pivot['Outlier'] = pedidos_pivot.iloc[:, -6] > pedidos_pivot.Corte

for i in range(0, len(pedidos_pivot)):
    if pedidos_pivot.iloc[i]['Outlier']:
        continue
    else:
        if pd.isna(pedidos_pivot.iloc[i][-7:-6].values[0]):
            pedidos_pivot.loc[i, 'Outlier'] = False
        else:
            if pedidos_pivot.iloc[i][-7:-6].values[0] / pedidos_pivot.iloc[i][2:-7].sum() > fator_outlier:
                pedidos_pivot.loc[i, 'Outlier'] = True
            else:
                continue

for i in pedidos_pivot[(pedidos_pivot.Outlier) & ((pedidos_pivot.Corte < 2) | (pedidos_pivot.iloc[:,12] < 2))].index:
    pedidos_pivot.loc[i, 'Outlier'] = False

semana_num = pedidos_pivot.iloc[i][-7:-6].index[0]
pedidos_pivot_home_decor_quantidade = pedidos_pivot[(pedidos_pivot.Time == 'Home & Decor') & (pedidos_pivot.Outlier)].reset_index(drop = True)
pedidos_pivot_puericultura_quantidade = pedidos_pivot[(pedidos_pivot.Time == 'Puericultura') & (pedidos_pivot.Outlier)].reset_index(drop = True)



#FATURADO
pedidos_pivot = itens_pedidos.pivot_table(index = ['Item - Código do produto', 'Time'], columns = 'Semana', values = 'Item - Valor líquido', aggfunc = 'sum').reset_index()

pedidos_pivot['Q1'], pedidos_pivot['Q3'], pedidos_pivot['Corte'] = np.nan, np.nan, np.nan

#Fazendo calculo de IIQ e Corte
for i in range(0, len(pedidos_pivot)):
    pedidos_pivot.loc[i, 'Q1'] = pedidos_pivot.iloc[i][2:-2].quantile(0.25)
    pedidos_pivot.loc[i, 'Q3'] = pedidos_pivot.iloc[i][2:-2].quantile(0.75)
    pedidos_pivot.loc[i, 'IIQ'] = pedidos_pivot.iloc[i][2:-2].quantile(0.75) - pedidos_pivot.iloc[i][2:-2].quantile(0.25)
    pedidos_pivot.loc[i, 'Corte'] = pedidos_pivot.iloc[i][2:-2].quantile(0.75) + ((pedidos_pivot.iloc[i][2:-2].quantile(0.75) - pedidos_pivot.iloc[i][2:-2].quantile(0.25)) * fator_outlier)

pedidos_pivot['Outlier Faturamento'] = pedidos_pivot.iloc[:, -6] > pedidos_pivot.Corte


for i in range(0, len(pedidos_pivot)):
    if pedidos_pivot.iloc[i]['Outlier Faturamento']:
        continue
    else:
        if pd.isna(pedidos_pivot.iloc[i][-7:-6].values[0]):
            pedidos_pivot.loc[i, 'Outlier Faturamento'] = False
        else:
            if pedidos_pivot.iloc[i][-7:-6].values[0] / pedidos_pivot.iloc[i][2:-7].sum() > 0.09:
                pedidos_pivot.loc[i, 'Outlier Faturamento'] = True
            else:
                continue

for i in pedidos_pivot[(pedidos_pivot['Outlier Faturamento']) & ((pedidos_pivot.Corte < 2) | (pedidos_pivot.iloc[:,12] < 2))].index:
    pedidos_pivot.loc[i, 'Outlier Faturamento'] = False

pedidos_pivot_home_decor_faturamento = pedidos_pivot[(pedidos_pivot.Time == 'Home & Decor') & (pedidos_pivot['Outlier Faturamento'])].reset_index(drop = True)
pedidos_pivot_puericultura_faturamento = pedidos_pivot[(pedidos_pivot.Time == 'Puericultura') & (pedidos_pivot['Outlier Faturamento'])].reset_index(drop = True)


from plyer import notification
notification.notify(title='Cálculo de outliers refeito', message='Veja a nova lista de outliers', timeout=5)


#Fazendo tratamentos para envio
pedidos_pivot_puericultura_quantidade['SKU BBTudo'] = pedidos_pivot_puericultura_quantidade['Item - Código do produto'].isin(skus_bbtudo.SKU)
pedidos_pivot_puericultura_faturamento['SKU BBTudo'] = pedidos_pivot_puericultura_faturamento['Item - Código do produto'].isin(skus_bbtudo.SKU)

pedidos_pivot_puericultura_quantidade.drop(columns = ['Q1', 'Q3', 'Corte', 'IIQ', 'Outlier'])
pedidos_pivot_puericultura_faturamento.drop(columns = ['Q1', 'Q3', 'Corte', 'IIQ', 'Outlier Faturamento'])
pedidos_pivot_home_decor_quantidade.drop(columns = ['Q1', 'Q3', 'Corte', 'IIQ', 'Outlier'])
pedidos_pivot_home_decor_faturamento.drop(columns = ['Q1', 'Q3', 'Corte', 'IIQ', 'Outlier Faturamento'])

#Criando o arquivo
with pd.ExcelWriter('Outliers.xlsx', engine='xlsxwriter') as writer:
    pedidos_pivot_puericultura_quantidade.to_excel(writer, sheet_name='Puericultura Qtde', index=False)
    pedidos_pivot_home_decor_quantidade.to_excel(writer, sheet_name='Home & Decor Qtde', index=False)
    pedidos_pivot_puericultura_faturamento.to_excel(writer, sheet_name='Puericultura Faturado', index=False)
    pedidos_pivot_home_decor_faturamento.to_excel(writer, sheet_name='Home & Decor Faturado', index=False)



html_formatado = '''<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Notificação - Outliers</title>
    <style>
        body {
            font-family: 'Roboto', sans-serif;
            background-color: #eaeff2;
            margin: 0;
            padding: 0;
        }
        .container {
            max-width: 500px;
            background-color: #fff;
            padding: 40px;
            border-radius: 30px;
            box-shadow: 0 5px 15px rgba(0, 0, 0, 0.1);
            margin: 50px auto;
            border: 3px solid #5a1aba;
        }
        h1 {
            font-size: 26px;
            color: #5a1aba;
            margin-bottom: 15px;
            text-align: left;
            letter-spacing: 1px;
        }
        p {
            font-size: 17px;
            color: #444;
            line-height: 1.7;
            margin-bottom: 25px;
        }
        .highlight {
            background-color: #f1f8ff;
            border-left: 5px solid #5a1aba;
            padding: 15px;
            margin-bottom: 20px;
            color: #333;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>Outliers</h1>
        <div class="highlight">
            <p>Arquivo de Outliers Atualizado!</p>
        </div>
    </div>
</body>
</html>
'''
disparar_email(html_formatado, semana = semana_num)