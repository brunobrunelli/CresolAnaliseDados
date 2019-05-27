# Projeto Análise de Dados - Cresol Confederação

Desenvolvido por: Bruno Scovoli Bruneli

Data: 26/05/2019

E-mail: brunobrunelli.bb@gmail.com

LinkedIn: https://www.linkedin.com/in/brunobruneli/

---

#### Overview do Projeto

Neste projeto estaremos desenvolvendo uma lógica em Python para extração, transformação e carregamento (ETL) dos dados de um arquivo compactado (gzip) de Log de Serviços de Hospedagem. Onde será analisado e extraído informações estatísticas para acompanhamento do processo de geração de log.

Informações de Saída em um arquivo .csv em mesma order dos itens da lista abaixo com base na análise dos dados de Log:

1. os 5 (cinco) logins que mais efetuaram requisições;
2. os 10 (dez) browsers mais utilizados;
3. os endereços de rede (classe C) com maior quantidade de requisições;
4. a hora com mais acesso no dia;
5. a hora com a maior quantidade de bytes;
6. o endpoint com maior consumo de bytes;
7. a quantidade de bytes por minuto;
8. a quantidade de bytes por hora;
9. a quantidade de usuários por minuto;
10. a quantidade de usuários por hora;
11. a quantidade de requisições que tiveram erro de cliente, agrupadas por erro;
12. a quantidade de requisições que tiveram sucesso;
13. a quantidade de requisições que foram redirecionadas;

### Ferramentas Utilizadas

* Python 3.7.3 / Pip 19.0.3
* Jupyter Notebook
* Sublime Text 3

### Módulos e Bibliotecas utilizadas

* shlex / pandas / csv / gzip / shutil / datetime 

### Criando o Ambiente - (Virtualenv)

- Comando para instalação do virtualenv:
`pip install virtualenv`

- Criando um novo ambiente com o virtualenv chamado de "Projeto_Cresol_Analise_Dados":
`virtualenv Projeto_Cresol_Analise_Dados`

- Ativando o ambiente virtualenv na pasta "Projeto_Cresol_Analise_Dados\Scripts\":
`activate.bat`

- Com o ambiente isolado ativado, foi gerado o arquivo de dependencias a partir do comando:
`pip freeze > requirements.txt`

- Desativando o ambiente virtualenv na pasta "Projeto_Cresol_Analise_Dados\Scripts\":
`deactivate`

- Para instalar as dependências do projeto no ambiente isolado, basta executar:
`pip install -r requirements.txt`

### Intruções de Execução

1. Deverá ser realizado um gitClone do projeto disponível no diretório Git abaixo, podemos navegar até a pasta "..\Projeto_Cresol_Analise_Dados\Python" onde encontraremos o arquivo - "Analise_Dados_Cresol_Python.py". Esse arquivo poderá ser executado via cmd através do comando "python Analise_Dados_Cresol_Python.py".

- [Projeto Git](https://github.com/brunobrunelli/CresolAnaliseDadosGit.git)
- [Arquivo .py](https://github.com/brunobrunelli/CresolAnaliseDadosGit/blob/master/Projeto_Cresol_Analise_Dados/Python/Analise_Dados_Cresol_Python.py)
- [Arquivo Notebook](https://github.com/brunobrunelli/CresolAnaliseDadosGit/blob/master/Projeto_Cresol_Analise_Dados/Python/Analise_Dados_Cresol_Python.ipynb)

2. O processo de extração e análise dos dados será iniciado informando na tela a data e hora do mesmo.
3. Após o processo ser concluído e informado na tela a data e horário de conclusão, será gerado um arquivo ".csv" com os dados analisados conforme descritos e requisitados pelo projeto.
4. O arquivo estará disponível na mesma pasta em que o projeto ".py" se encontra.

### Conclusão

Com este projeto podemos aperfeiçoar e expandir nosso conhecimento na linguagem python, junto a análise e extração de informações para tomada de decisão, onde o mesmo será encaminhado aos profissionais da área de dados da empresa Cresol para análise da lógica e resultados obtidos para o processo de seleção da vaga de Analista de Dados. Contudo espero que toda lógida e estrura do projeto atendam as espectativas necessárias para uma possível contratação.

Att Bruno Scovoli Bruneli













