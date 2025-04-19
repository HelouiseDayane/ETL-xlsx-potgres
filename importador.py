import os
import shutil
import pandas as pd
from pathlib import Path
from etl_process import etl_transformacao, exibir_resumo

UPLOAD_DIR = Path("uploads")
UPLOAD_DIR.mkdir(exist_ok=True)

def receber_arquivo_excel():
    print("🔍 Insira o caminho completo do arquivo .xlsx que deseja importar:")
    caminho = input("📁 Caminho do arquivo: ").strip()

    if not os.path.exists(caminho):
        print("❌ Arquivo não encontrado.")
        return None

    if not caminho.endswith(".xlsx"):
        print("❌ O arquivo precisa ser um .xlsx válido.")
        return None

    nome_arquivo = os.path.basename(caminho)
    destino = UPLOAD_DIR / nome_arquivo

    shutil.copy(caminho, destino)
    print(f"✅ Arquivo copiado para: {destino}")

    return destino

def visualizar_excel(caminho_arquivo):
    try:
        print("\n📄 Lendo as planilhas disponíveis no arquivo...")
        excel = pd.ExcelFile(caminho_arquivo)
        print("🔢 Planilhas encontradas:", excel.sheet_names)

        for sheet in excel.sheet_names:
            print(f"\n📑 Primeiras linhas da planilha: {sheet}")
            df = pd.read_excel(excel, sheet_name=sheet)
            print(df.head(5)) 

    except Exception as e:
        print("❌ Erro ao ler o arquivo:", e)


def processar_arquivo(caminho_arquivo):
    """ Função para processar o arquivo após ser recebido """
    df = pd.read_excel(caminho_arquivo)

    registros_importados, registros_nao_importados = etl_transformacao(df)
    exibir_resumo(registros_importados, registros_nao_importados)

if __name__ == "__main__":
    caminho = receber_arquivo_excel()
    if caminho:
        visualizar_excel(caminho) 
        processar_arquivo(caminho)  
