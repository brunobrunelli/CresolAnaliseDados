
### Análise de Dados - Desafio Cresol Confederação

# Desenvolvedor: Bruno Scovoli Bruneli
# Data: 26/05/2019
# Email: brunobrunelli.bb@gmail.com
# LinkedIn: https://www.linkedin.com/in/brunobruneli/

# Descrição do Projeto:
# Este código tem como objetivo a extração, limpesa, transformação, 
# análise e carregamento de todo conteúdo extraído de um arquivo de 
# log de serviços de hospedagem proposto pelo teste de conhecimento 
# da Cresol Confederação.  

# --------------------------------------------------------------------------------------------------------------------

# Imports:
import pandas as pd
import os
from datetime import datetime 

# Função para extração e análise dos dados do arquivo de log.gz:
def extrair_analisar_dados_arquivo():
    
    # Criação de um dataframe a partir da leitura de dados de um arquivo.log:
    df_data = pd.read_csv('apache.log.gz',compression = 'gzip',sep = " ",header = None,\
        names = ['ip_address', 'ifen', 'user', 'user_code', 'timestamp','request', 'html_code',\
         'size_object', 'url_refer', 'browser']
        ).drop(columns=['ifen'])
    
    
    # ----------------------------------------------------------------------------------------------------------------
    # Reestruturação e modificação das colunas do dataframe:

    # Formatação da coluna de data e hora:
    df_data.timestamp = pd.to_datetime(df_data.timestamp, format='[%Y-%m-%dT%H:%M:%SZ]')
    
    # Definindo a coluna de data e hora como índice do dataframe:
    df_data.set_index(df_data.timestamp, inplace=True)

    # Extraindo somente o nome dos browsers:  
    df_data.browser = df_data.browser.apply(lambda x: x.split(" ")[0])

    # ----------------------------------------------------------------------------------------------------------------
    # Lista de dados esperado como saída:

    # os 5 (cinco) logins que mais efetuaram requisições;
    top_logins = df_data.groupby(by='user').size().sort_values(ascending=False).head().reset_index().user
    top_logins = list(top_logins)

    # os 10 (dez) browsers mais utilizados;
    top_browsers = df_data.groupby(by='browser').size().sort_values(ascending=False).head(10).reset_index().browser
    top_browsers = list(top_browsers)

    # os endereços de rede (classe C) com maior quantidade de requisições;
    top_endereco_rede = df_data.loc[df_data.ip_address.apply(lambda x: int(x.split('.')[0]) >= 192 and int(x.split('.')[0]) <= 224)]\
    .groupby(by='ip_address').size().sort_values(ascending=False).reset_index().ip_address
    top_endereco_rede = list(top_endereco_rede)

    # a hora com mais acesso no dia;
    hora_acesso_dia = df_data.resample('H').size().sort_values(ascending=False).index[0].hour

    # a hora com a maior quantidade de bytes;
    hora_bytes = df_data.size_object.resample('H').sum().sort_values(ascending=False).index[0].hour
    
    # o endpoint com maior consumo de bytes;
    maior_endpoint = df_data.assign(endpoint = df_data.request.apply(lambda x: x.split("/")[1]))\
    .groupby('endpoint').size().sort_values(ascending=False).index[0]
    
    # a quantidade de bytes por minuto;
    qtd_bytes_min = df_data.size_object.resample('Min').sum().mean()

    # a quantidade de bytes por hora;
    qtd_bytes_hora = df_data.size_object.resample('H').sum().mean()

    # a quantidade de usuários por minuto;
    qtd_user_min = df_data.user.resample('Min').size().mean()

    # a quantidade de usuários por hora;
    qtd_user_hora = df_data.user.resample('H').size().mean()

    # a quantidade de requisições que tiveram erro de cliente, agrupadas por erro;
    qtd_rec_by_cliente = df_data.loc[df_data.html_code.apply(lambda x: int(x)>= 400 and int(x) <= 499)]\
    .groupby('html_code').size().sort_values(ascending=False).to_dict()

    # a quantidade de requisições que tiveram sucesso;
    qtd_req_sucesso = df_data.loc[df_data.html_code.apply(lambda x: int(x) >= 200 and int(x) <= 226 )].shape[0]

    # a quantidade de requisições que foram redirecionadas;
    qtd_req_redirecionada = df_data.loc[df_data.html_code.apply(lambda x: int(x)>= 300 and int(x) <= 308)].shape[0]

    df_output = pd.DataFrame(
        {
        'top_logins': [top_logins],
        'top_browsers': [top_browsers],
        'top_endereco_rede': [top_endereco_rede],
        'hora_acesso_dia': [hora_acesso_dia],
        'hora_bytes': [hora_bytes],
        'maior_endpoint': [maior_endpoint],
        'qtd_bytes_min': [qtd_bytes_min],
        'qtd_bytes_hora': [qtd_bytes_hora],
        'qtd_user_min': [qtd_user_min],
        'qtd_user_hora': [qtd_user_hora],
        'qtd_rec_by_cliente': [qtd_rec_by_cliente],
        'qtd_req_sucesso': [qtd_req_sucesso],
        'qtd_req_redirecionada': [qtd_req_redirecionada]
        }
    )

    return df_output

# Função que realiza a gravação dos dados em um arquivo .csv:
def grava_dados_csv(lista_dados):
    lista_dados.to_csv('resultado_analise_log.csv', index=False)


print('Processo Iniciado - %s ...\n' %str(datetime.now()))

# Chamada das funções criadas acima, passando os parametros esperados:
dados_extraidos = extrair_analisar_dados_arquivo()
grava_dados_csv(dados_extraidos)

print('Processo Conclído - %s ...' %str(datetime.now()))
