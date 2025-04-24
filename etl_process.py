import pandas as pd
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_date, lit
import psycopg2
from psycopg2 import sql
from datetime import datetime


def limpar_cpf_cnpj(cpf_cnpj):
    return re.sub(r'\D', '', str(cpf_cnpj))


def limpar_celular(celular):
    return str(celular).replace("+", "").replace("-", "").strip()


def get_valor(row, coluna, default=''):
    valor = row.get(coluna)
    return default if pd.isna(valor) else valor


def conectar_banco():
    return psycopg2.connect(
        dbname="etl_tsmx", user="postgres", password="postgres", host="localhost", port="5432"
    )


def inserir_planos_unicos(df):
    try:
        conn = conectar_banco()
        cur = conn.cursor()

        planos_unicos = df[['Plano', 'Plano_Valor']].drop_duplicates()

        for _, row in planos_unicos.iterrows():
            cur.execute("""
                INSERT INTO tbl_planos (descricao, valor)
                VALUES (%s, %s)
                ON CONFLICT (descricao) DO NOTHING
            """, (row['Plano'], row['Plano_Valor']))

        conn.commit()
        print("✅ Planos inseridos com sucesso!")

    except Exception as e:
        print("❌ Erro ao inserir planos:", e)
    finally:
        cur.close()
        conn.close()


def buscar_id(cur, tabela, coluna_busca, valor):
    cur.execute(sql.SQL("SELECT id FROM {} WHERE {} = %s").format(
        sql.Identifier(tabela),
        sql.Identifier(coluna_busca)
    ), (valor,))
    resultado = cur.fetchone()
    return resultado[0] if resultado else None


def inserir_dados_banco(df):
    registros_importados = 0
    registros_nao_importados = []

    try:
        conn = conectar_banco()
        cur = conn.cursor()

        for _, row in df.iterrows():
            try:
                cur.execute("""
                    INSERT INTO tbl_clientes (nome_razao_social, nome_fantasia, cpf_cnpj, data_nascimento, data_cadastro)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (cpf_cnpj) DO NOTHING
                """, (
                    row['Nome_Razão_Social'], row['Nome_Fantasia'], row['cpf_cnpj'], row['Data_Nasc'], row['Data_Cadastro_cliente']
                ))
                registros_importados += 1
            except Exception as e:
                registros_nao_importados.append({"linha": row.to_dict(), "erro": str(e)})

        conn.commit()
    except Exception as e:
        print("❌ Erro ao conectar com o banco de dados:", e)
    finally:
        cur.close()
        conn.close()

    return registros_importados, registros_nao_importados


def inserir_contratos(df):
    contratos_inseridos = 0
    contratos_nao_inseridos = []

    try:
        conn = conectar_banco()
        cur = conn.cursor()

        for _, row in df.iterrows():
            try:
                cur.execute("SAVEPOINT before_insert")

                cliente_id = buscar_id(cur, "tbl_clientes", "cpf_cnpj", row['cpf_cnpj'])
                if not cliente_id:
                    raise Exception("Cliente não encontrado")

                plano_id = buscar_id(cur, "tbl_planos", "descricao", row['Plano'])
                if not plano_id:
                    raise Exception("Plano não encontrado")

                cur.execute("""
                    INSERT INTO tbl_cliente_contratos (
                        cliente_id, plano_id, dia_vencimento, isento,
                        endereco_logradouro, endereco_numero, endereco_bairro,
                        endereco_cidade, endereco_complemento, endereco_cep, endereco_uf, status_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    cliente_id, plano_id, row.get("Vencimento") or 10, False,
                    get_valor(row, "Endereço"), get_valor(row, "Número"), get_valor(row, "Bairro"),
                    get_valor(row, "Cidade"), get_valor(row, "Complemento"), get_valor(row, "CEP"),
                    get_valor(row, "UF", "")[:2], 1
                ))
                contratos_inseridos += 1

            except Exception as e:
                cur.execute("ROLLBACK TO SAVEPOINT before_insert")
                contratos_nao_inseridos.append({"linha": row.to_dict(), "erro": str(e)})

        conn.commit()
    except Exception as e:
        print("❌ Erro ao inserir contratos:", e)
    finally:
        cur.close()
        conn.close()

    print(f"✅ Contratos inseridos: {contratos_inseridos}")
    if contratos_nao_inseridos:
        print(f"❌ Contratos não inseridos: {len(contratos_nao_inseridos)}")
        for item in contratos_nao_inseridos:
            print(f"Erro: {item['erro']} | Linha: {item['linha']}")


def inserir_contatos(df):
    contatos_inseridos = 0
    contatos_nao_inseridos = []

    try:
        conn = conectar_banco()
        cur = conn.cursor()

        tipos_contatos = {
            'Telefone': ('Telefones', 1),
            'Celular': ('Celulares', 2),
            'E-Mail': ('Emails', 3),
        }

        for _, row in df.iterrows():
            try:
                cur.execute("SAVEPOINT before_insert")

                cliente_id = buscar_id(cur, "tbl_clientes", "cpf_cnpj", row['cpf_cnpj'])
                if not cliente_id:
                    raise Exception("Cliente não encontrado")

                for tipo, (coluna_excel, tipo_id) in tipos_contatos.items():
                    valor = row.get(coluna_excel)
                    if pd.notna(valor):
                        cur.execute("""
                            INSERT INTO tbl_cliente_contatos (cliente_id, tipo_contato_id, contato)
                            VALUES (%s, %s, %s)
                        """, (cliente_id, tipo_id, str(valor).strip()))
                        contatos_inseridos += 1

            except Exception as e:
                cur.execute("ROLLBACK TO SAVEPOINT before_insert")
                contatos_nao_inseridos.append({"linha": row.to_dict(), "erro": str(e)})

        conn.commit()
    except Exception as e:
        print("❌ Erro ao inserir contatos:", e)
    finally:
        cur.close()
        conn.close()

    print(f"✅ Contatos inseridos: {contatos_inseridos}")
    if contatos_nao_inseridos:
        print(f"❌ Contatos não inseridos: {len(contatos_nao_inseridos)}")
        for item in contatos_nao_inseridos:
            print(f"Erro: {item['erro']} | Linha: {item['linha']}")


def etl_transformacao(df):
    print("📄 Iniciando limpeza com Pandas...")

    df.columns = [col.strip().replace(" ", "_").replace(".", "").replace("/", "_") for col in df.columns]

    df["cpf_cnpj"] = df["CPF_CNPJ"].apply(limpar_cpf_cnpj)
    df["Celulares"] = df["Celulares"].apply(limpar_celular)

    spark = SparkSession.builder.appName("ETL Process").getOrCreate()
    spark_df = spark.createDataFrame(df)

    print("🧹 Transformando os dados com Spark...")

    spark_df = spark_df.withColumn("Data_Nasc", to_date(col("Data_Nasc"), 'yyyy-MM-dd')) \
                       .withColumn("Data_Cadastro_cliente", to_date(col("Data_Cadastro_cliente"), 'yyyy-MM-dd')) \
                       .withColumn("Plano_Valor", when(col("Plano_Valor").isNotNull(), col("Plano_Valor").cast("float"))) \
                       .withColumn("UF", when(col("UF").isNotNull(), col("UF")).otherwise(lit("Desconhecido"))) \
                       .withColumn("Plano", when(col("Plano").isNotNull(), col("Plano")).otherwise(lit("Plano Desconhecido")))

    spark_df = spark_df.dropDuplicates(["cpf_cnpj"])

    print(f"🔍 Registros após deduplicação: {spark_df.count()}")

    df_clean = spark_df.toPandas()

    inserir_planos_unicos(df_clean)
    registros_importados, registros_nao_importados = inserir_dados_banco(df_clean)
    inserir_contratos(df_clean)
    inserir_contatos(df_clean)

    return registros_importados, registros_nao_importados


def exibir_resumo(registros_importados, registros_nao_importados):
    print(f"\n🔢 Total de registros importados: {registros_importados}")

    if registros_nao_importados:
        print("\n❌ Registros não importados:")
        for item in registros_nao_importados:
            print(f"Erro ao importar: {item['erro']} - Linha: {item['linha']}")
    else:
        print("✅ Todos os registros foram importados com sucesso!")
