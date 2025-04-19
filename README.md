# **🧪 ETL Process - TSMX**
Este projeto implementa um processo de ETL (Extração, Transformação e Carga) utilizando Python, Pandas, PySpark e PostgreSQL. O pipeline extrai dados de um arquivo Excel, realiza tratamentos e transformações, e insere os dados limpos no banco de dados.

# **⚙️ Requisitos**


*   Docker e Docker Compose
*   Python 3.10+
*   Pip


# **📦 Instalação e Execução**
**1. Clone o repositório**

```
git clone https://github.com/seu-usuario/ETL-xlsx-postgres.git
cd ETL-xlsx-postgres
```


**2. Suba o banco de dados PostgreSQL com Docker**


```
docker-compose up -d
```



Este comando irá:

*   Criar um container PostgreSQL com a base de dados etl_tsmx
*   Executar o script schema_database_pgsql.sql automaticamente para criar as tabelas necessárias





**3. Instale as dependências Python**
De preferência, crie um ambiente virtual:

```
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
```

Instale os pacotes:
```
pip install -r requirements.txt
```


 **4. Adicione o arquivo de dados**
Salve seu Excel com os dados de entrada no caminho:

```
/caminhosdoseuqarquivo/Documentos/python/dados_importacao.xlsx

```



**5. Execute o pipeline ETL**
```
python etl_process.py

```

# **🔁 O que o pipeline faz?**
 **📥 Extração**
*   Lê o arquivo dados_importacao.xlsx com pandas.

**🧹 Transformação**
*   Limpa CPF/CNPJ e celulares com expressões regulares

*   Ajusta colunas com pyspark:

*   Formata datas

*   Preenche campos nulos com valores padrão

*   Converte tipos de dados

*   Remove duplicatas por CPF/CNPJ

**📤 Carga**
*   Insere os dados nas seguintes tabelas PostgreSQL:

  * tbl_clientes

  * tbl_cliente_contatos

  * tbl_cliente_contratos

  * tbl_planos

**🗃 Estrutura de banco**
As seguintes tabelas são esperadas:

* tbl_clientes: cadastro de pessoas físicas ou jurídicas

* tbl_planos: planos contratados

* tbl_cliente_contratos: relação entre cliente e plano

* tbl_cliente_contatos: contatos como telefone, celular e email

**🐛 Logs e Tratamento de Erros**
* Linhas com erro durante a carga são mostradas com detalhes no console.

* Pontos de falha são isolados com SAVEPOINT e ROLLBACK para não afetar toda a transação.

* Mensagens claras indicam sucesso ou falha de cada etapa.

**📌 Observações**
* O banco espera que os CPFs/CNPJs sejam únicos (ON CONFLICT DO NOTHING).

* A ordem de inserção é importante: primeiro clientes → depois planos, contratos e contatos.

* Os dados devem estar bem estruturados e consistentes no Excel para evitar falhas de inserção.

🧑‍💻 Autor
Desenvolvido por [Helouise Dayane](https://www.helouisedayane.top)

