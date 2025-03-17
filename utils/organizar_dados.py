import pandas as pd
import re

def organizar_dados(arquivo_entrada, arquivo_saida):
    """
    Organiza dados de uma planilha onde as informações estão em uma única coluna
    para um formato onde cada linha representa um arquivo com suas respectivas informações.
    
    Args:
        arquivo_entrada: Caminho para o arquivo de entrada (txt, csv, etc)
        arquivo_saida: Caminho para salvar o arquivo de saída em formato CSV
    """
    # Leitura do arquivo de entrada
    with open(arquivo_entrada, 'r', encoding='utf-8') as file:
        linhas = file.readlines()
    
    # Listas para armazenar os dados extraídos
    filenames = []
    fileentryids = []
    companyids = []
    folderids = []
    
    # Variáveis para armazenar dados do registro atual
    filename = None
    fileentryid = None
    companyid = None
    folderid = None
    
    # Processa cada linha
    for linha in linhas:
        linha = linha.strip()
        
        # Se encontrar uma nova linha de FILENAME e já tiver um filename registrado,
        # significa que terminou um registro e deve salvar os dados coletados
        if linha.startswith("FILENAME:") and filename is not None:
            filenames.append(filename)
            fileentryids.append(fileentryid)
            companyids.append(companyid)
            folderids.append(folderid)
            
            # Reseta para o próximo registro
            filename = None
            fileentryid = None
            companyid = None
            folderid = None
        
        # Extrai as informações da linha atual
        if linha.startswith("FILENAME:"):
            filename = linha.replace("FILENAME:", "").strip()
        elif linha.startswith("FILEENTRYID:"):
            fileentryid = linha.replace("FILEENTRYID:", "").strip()
        elif linha.startswith("COMPANYID:"):
            companyid = linha.replace("COMPANYID:", "").strip()
        elif linha.startswith("FOLDERID:"):
            folderid = linha.replace("FOLDERID:", "").strip()
    
    # Adiciona o último registro se houver dados
    if filename is not None:
        filenames.append(filename)
        fileentryids.append(fileentryid)
        companyids.append(companyid)
        folderids.append(folderid)
    
    # Cria um DataFrame com os dados extraídos
    df = pd.DataFrame({
        'FILENAME': filenames,
        'FILEENTRYID': fileentryids,
        'COMPANYID': companyids,
        'FOLDERID': folderids
    })
    
    # Salva o DataFrame em um arquivo CSV
    df.to_csv(arquivo_saida, index=False)
    
    print(f"Total de registros processados: {len(filenames)}")
    
    return df

# Exemplo de uso
if __name__ == "__main__":
    # Caminho absoluto para os arquivos
    arquivo_entrada = "/home/sea/saude-fix/fix-saude/dados/dados_entrada.txt"
    arquivo_saida = "/home/sea/saude-fix/fix-saude/dados/dados_organizados.csv"
    
    df_organizado = organizar_dados(arquivo_entrada, arquivo_saida)
    print("Dados organizados com sucesso!")
    print(df_organizado.head())  # Mostra as primeiras linhas do DataFrame organizado   