
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

# Extração de dados do arquivo de log.gz:
def extrair_dados_arquivo():
    # Criação de um dataframe a partir da leitura de dados de um arquivo.log:
    df_data = pd.read_csv('apache.log.gz',compression = 'gzip',sep = " ",header = None,\
        names = ['ip_address', 'ifen', 'user', 'user_code', 'timestamp','request', 'html_code',\
         'size_object', 'url_refer', 'browser']
        ).drop(columns=['ifen'])

    return df_data

# Reestruturação e modificação das colunas do dataframe:
def modifica_dados_dataframe(df_data):
    
    # Formatação da coluna de data e hora:
    df_data.timestamp = pd.to_datetime(df_data.timestamp, format='[%Y-%m-%dT%H:%M:%SZ]')
    
    # Definindo a coluna de data e hora como índice do dataframe:
    df_data.set_index(df_data.timestamp, inplace=True)

    # Extraindo somente o nome dos browsers:  
    df_data.browser = df_data.browser.apply(lambda x: x.split(" ")[0])
    
    return df_data

# Função para extração e análise dos dados do arquivo de log.gz:
def analisar_dados_dataframe(df_data):
    
    # -----------------------------------------------------------------------------------------------------------------
    
    # Realizamos um Groupby do dataframe pela coluna "USER" retornando em formato de lista:
    top_logins = df_data.groupby(by='user').size().sort_values(ascending=False).head().reset_index().user
    top_logins = list(top_logins)
    
    # Realizamos um Groupby do dataframe pela coluna "BROWSER" retornando os 10 primeiros registros em formato de lista:
    top_browsers = df_data.groupby(by='browser').size().sort_values(ascending=False).head(10).reset_index().browser
    top_browsers = list(top_browsers)

    # Realizamos um Groupby do dataframe pela coluna "IP_ADDRESS" retornando os ips de classe "C" em formato de lista:
    top_endereco_rede = df_data.loc[df_data.ip_address.apply(lambda x: int(x.split('.')[0]) >= 192 and int(x.split('.')[0]) <= 224)]\
    .groupby(by='ip_address').size().sort_values(ascending=False).reset_index().ip_address
    top_endereco_rede = list(top_endereco_rede)

    # Realizamos um resample do dataframe pela "HORA" retornando a hora de mais acesso no dia:
    hora_acesso_dia = df_data.resample('H').size().sort_values(ascending=False).index[0].hour

    # Realizamos um resample do dataframe pela "HORA" retornando a hora de maior tamanho em bytes:
    hora_bytes = df_data.size_object.resample('H').sum().sort_values(ascending=False).index[0].hour
    
    # Realizamos um Groupby do dataframe pela coluna "ENDPOINT" retornando o endpoint de maior tamanho:
    maior_endpoint = df_data.assign(endpoint = df_data.request.apply(lambda x: x.split("/")[1]))\
    .groupby('endpoint').size().sort_values(ascending=False).index[0]
    
    # Realizamos um resample do dataframe pelo "MINUTO" retornando a quantidade de minutos de maior tamanho:
    qtd_bytes_min = df_data.size_object.resample('Min').sum().mean()
    
    # Realizamos um resample do dataframe pelo "HORA" retornando a quantidade de horas de maior tamanho:
    qtd_bytes_hora = df_data.size_object.resample('H').sum().mean()

    # Realizamos um resample do dataframe pelo "MINUTO" retornando a quantidade de minutos por usuário:
    qtd_user_min = df_data.user.resample('Min').size().mean()

    # Realizamos um resample do dataframe pelo "HORA" retornando a quantidade de horas por usuário:
    qtd_user_hora = df_data.user.resample('H').size().mean()

    # Realizamos um Groupby do dataframe pela coluna "HTML_CODE" retornando dicionario com "erro de cliente":
    qtd_req_by_cliente = df_data.loc[df_data.html_code.apply(lambda x: int(x)>= 400 and int(x) <= 499)]\
    .groupby('html_code').size().sort_values(ascending=False).to_dict()

    # Realizamos um Groupby do dataframe pela coluna "HTML_CODE" retornando dicionario com "sucessos":
    qtd_req_sucesso = df_data.loc[df_data.html_code.apply(lambda x: int(x) >= 200 and int(x) <= 226 )].shape[0]

    # Realizamos um Groupby do dataframe pela coluna "HTML_CODE" retornando dicionario com "redirecionadas":
    qtd_req_redirecionada = df_data.loc[df_data.html_code.apply(lambda x: int(x)>= 300 and int(x) <= 308)].shape[0]
    
    # -----------------------------------------------------------------------------------------------------------------
    
    # Lista de dados esperado como saída:
    # os 5 (cinco) logins que mais efetuaram requisições;
    # os 10 (dez) browsers mais utilizados;
    # os endereços de rede (classe C) com maior quantidade de requisições;
    # a hora com mais acesso no dia;
    # a hora com a maior quantidade de bytes;
    # o endpoint com maior consumo de bytes;
    # a quantidade de bytes por minuto;
    # a quantidade de bytes por hora;
    # a quantidade de usuários por minuto;
    # a quantidade de usuários por hora;
    # a quantidade de requisições que tiveram erro de cliente, agrupadas por erro;
    # a quantidade de requisições que tiveram sucesso;
    # a quantidade de requisições que foram redirecionadas;
    
    
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
        'qtd_req_by_cliente': [qtd_req_by_cliente],
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
df = extrair_dados_arquivo()
df = modifica_dados_dataframe(df)
lista = analisar_dados_dataframe(df)
grava_dados_csv(lista)

print('Processo Conclído - %s ...' %str(datetime.now()))
