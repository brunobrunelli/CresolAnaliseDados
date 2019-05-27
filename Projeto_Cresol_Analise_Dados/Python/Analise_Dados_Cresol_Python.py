
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

# -----------------------------------------------------------------------------------------------------------------

# Importação dos Pacotes:

import shlex
import pandas as pd
import csv
from datetime import datetime 

# Extração de dados a partir de um arquivo ".log":

def extrair_dados_arquivo(nome_arquivo):
    # Lógica de leitura dos dados do arquivo de Logs:
    with open(nome_arquivo,mode="r",encoding="utf-8",newline="\r\n") as f:
        result = f.readlines()
        
    # Remove linhas em branco:
    for i in result:
        if i == '\r\n':
            result.remove('\r\n')
        
    return result


# Tramento dos dados de entrada extraídos do arquivo de log:

def tratar_dados(result):
    listaLog = []
    
    for i in range(len(result)):
        lista, user_agent_lista= [], []

        try:
            # Separando o conteudo dentro de cada item da lista:
            aux = shlex.split(result[i], posix=False)

            # Lógica para percorrer as 9 primeiras colunas, inserindo na lista:
            for f in range(10):
                # Removendo caracter:
                aux[f] = aux[f].replace('"','')

                # Lógica para remover a coluna '-':
                if f != 1 and f != 3:
                    
                    # Lógica para incluir duas novas colunas (DATE e TIME):
                    if f == 4: 
                        aux[f] = aux[f].replace('[','').replace(']','')
                        lista.append(datetime.strptime(aux[f],'%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S'))
                        
                    elif f == 5:
                        # Pegamos apenas primeiro conteúdo da lista criada a partir 'aux' após separarmos:
                        lista.append(aux[f].split(' ')[0])

                        # Pegamos somente o valor de endpoint:
                        lista.append('/'.join(aux[5].split(' ')[1].split('/')[0:2]))
                        
                    elif f == 9:
                        # Remove caracteres indesejados da coluna:
                        aux[f] = aux[f].replace('\r\n','')
                        lista.append(aux[f].split(' ')[0])
                        lista.append(aux[f])
                        
                    else:
                        lista.append(aux[f])

            # Incluímos os valores tratados da lista em uma sublista:
            listaLog.append(lista)
            
            # Excluímos as listas carregadas da memória para desalocar espaço:
            del lista
            del user_agent_lista

        except:
            print('Erro: A lista contém valores que não são esperados na linha - '+ str(i))
    
    return listaLog


# Análise dos dados:

def analisa_dados(lista_resultado):
    # Criação de um DataSet a partir dos dados extraídos do arquivo:
    
    lista_result_log = lista_resultado
    
    # Definindo uma lista com as Colunas do DataFrame:
    labels = ['IP_ADDRESS','USER','DATETIME','METHOD','ENDPOINT',\
              'STATUS_CODE','SIZE_OBJECT','REFER','BROWSER','USER_AGENT']
    
    # Criação do DataFrame a partir da lista de Logs:
    dfLog = pd.DataFrame(lista_result_log,columns=labels)
    
    pd.options.mode.chained_assignment = None
    
    # os 5 (cinco) logins que mais efetuaram requisições:
    top5_user = dfLog['USER'].sort_values(ascending=False).value_counts().head(5)
    top5_user = list(dict(top5_user).keys())
    
    # os 10 (dez) browsers mais utilizados:
    top10_browser = dfLog['BROWSER'].value_counts().head(10)
    top10_browser = list(dict(top10_browser).keys())
    
    # os endereços de rede (classe C) com maior quantidade de requisições:
    top10_end_req = dfLog['IP_ADDRESS'][(pd.to_numeric(dfLog['IP_ADDRESS'].str.split('.').str[0]) >= 192) & \
                                        (pd.to_numeric(dfLog['IP_ADDRESS'].str.split('.').str[0]) <= 223)]\
        .value_counts().head(10)

    top10_end_req = list(dict(top10_end_req).keys())
    
    # Selecionando somente as colunas que serão utilizadas:
    dfbymin = dfLog.iloc[:,1:8]
    
    # Convertendo as colunas para os formatos desejaveis:
    dfbymin["HORA"] = pd.to_datetime(dfbymin.loc[:,"DATETIME"]).dt.hour 
    dfbymin["MINUTE"] = pd.to_datetime(dfbymin.loc[:,"DATETIME"]).dt.minute
    dfbymin["SIZE_OBJECT"] = pd.to_numeric(dfLog["SIZE_OBJECT"])
    dfbymin["STATUS_CODE"] = pd.to_numeric(dfLog["STATUS_CODE"])
    
    # a hora com mais acesso no dia:
    hora_sucesso = dfbymin.iloc[:,[7]]["HORA"].value_counts().idxmax()
    
    # a hora com a maior quantidade de bytes:
    maior_hora_byte = dfbymin.groupby("HORA").agg("SIZE_OBJECT").mean().sort_values(ascending=False).idxmax()
    
    # o endpoint com maior consumo de bytes:
    endpoint_maior_byte = dfbymin.groupby("ENDPOINT").agg("SIZE_OBJECT").mean().sort_values(ascending=False).idxmax()
    
    # a quantidade de bytes por minuto:
    qtd_byte_min = dict(dfbymin.groupby("MINUTE").agg("SIZE_OBJECT").mean())
    
    # a quantidade de bytes por hora:
    qtd_byte_hora = dict(dfbymin.groupby('HORA').agg('SIZE_OBJECT').mean())
    
    # a quantidade de usuários por minuto:
    dfqtd_min = dfbymin.iloc[:,[0,8]].drop_duplicates()
    qtd_user_min = dict(dfqtd_min.groupby(["MINUTE"]).size())
    
    # a quantidade de usuários por hora:
    dfqtd_hora = dfbymin.iloc[:,[0,7]].drop_duplicates()
    qtd_user_hora = dict(dfqtd_hora.groupby(["HORA"]).size())
    
    # a quantidade de requisições que tiveram erro de cliente, agrupadas por erro:
    df_error = dfbymin.iloc[:,[0,4]][(dfbymin["STATUS_CODE"]>= 400) & (dfbymin["STATUS_CODE"]<= 499)]\
        .groupby(["STATUS_CODE"]).size().sort_values(ascending=False)

    req_cliente = dict(df_error)
    
    # a quantidade de requisições que tiveram sucesso:
    qtd_sucesso = list(dfbymin.iloc[:,[4]][(dfbymin["STATUS_CODE"]>= 200) & (dfbymin["STATUS_CODE"]<= 226)].count().values)
    
    # a quantidade de requisições que foram redirecionadas:
    qtd_redirecionado = list(dfbymin.iloc[:,[4]][(dfbymin["STATUS_CODE"]>= 300) & (dfbymin["STATUS_CODE"]<= 308)].count())
    
    # Criação de uma lista com os resultados das análises:
    lista_resultado = [top5_user,top10_browser,top10_end_req,hora_sucesso,maior_hora_byte,endpoint_maior_byte,\
                       qtd_byte_min,qtd_user_min,qtd_user_hora,req_cliente,qtd_sucesso,qtd_redirecionado]
    
    return lista_resultado


# Grava saída de dados em um arquivo CSV:

def grava_csv(nome_arquivo,lista_valores):
    # Retornamos a data atual para gravar no nome do arquivo:
    current_date = str(datetime.now().date())
    
    # Realizamos a gravação dos dados extraídos da análise do log:
    with open(str(nome_arquivo)+'_'+current_date+'.csv','w', newline='') as f:
        writer = csv.writer(f, delimiter=';') 
        writer.writerows(map(lambda x: [x], lista_valores))        


# Chamada das funções listadas acima:

# Realiza a leitura do arquivo:
result_data = extrair_dados_arquivo("apacheLogTest.log")

# Realiza a limpeza dos dados extraídos:
lista_result_log = tratar_dados(result_data)

# Realiza a análise dos dados:
lista_analise_log = analisa_dados(lista_result_log)

# Realiza a gravação dos dados:
grava_csv("apacheLogTest_analisado_by_BrunoBruneli",lista_analise_log)
