#!/usr/bin/env python3
# quick_download.py - Script modificado para baixar arquivos multipáginas

import aiohttp
import asyncio
import sys
import os
import ssl
import urllib3
import csv
import re
import shutil  # Para deletar a pasta de downloads
from urllib.parse import urljoin, urlparse, parse_qs, urlunparse
import traceback
import time
import datetime

# Importar o conversor AsyncImageToPdfConverter
from utils.image_to_pdf import AsyncImageToPdfConverter, setup_logging as setup_pdf_logging

# Desativar verificação SSL globalmente
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
ssl._create_default_https_context = ssl._create_unverified_context

def update_preview_index(url, index):
    """Atualiza o parâmetro previewFileIndex na URL"""
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    
    # Atualizar o parâmetro previewFileIndex
    query['previewFileIndex'] = [str(index)]
    
    # Reconstruir a URL
    updated_parsed = parsed._replace(query='&'.join(f"{k}={v[0]}" for k, v in query.items()))
    return urlunparse(updated_parsed)

async def download_file(url, output_dir, original_filename=None, page_num=1):
    """Download de um arquivo com SSL desativado, salvando como PNG"""
    if original_filename:
        # Usar o nome base do arquivo original, sem a extensão
        filename = original_filename.strip('"')
        # Remover a extensão existente e adicionar .png
        base_name = os.path.splitext(filename)[0]
        if page_num > 1:
            filename = f"{base_name}_page{page_num}.png"
        else:
            filename = f"{base_name}.png"
    else:
        # Fallback: extrair o nome do arquivo da URL e colocar extensão .png
        filename = url.split('/')[-1].split('?')[0]
        filename = os.path.splitext(filename)[0]
        if not filename:
            filename = f"file_{hash(url)}"
        if page_num > 1:
            filename = f"{filename}_page{page_num}.png"
        else:
            filename += '.png'
    
    output_path = os.path.join(output_dir, filename)
    
    try:
        # Criar diretório se não existir
        os.makedirs(output_dir, exist_ok=True)
        
        # Configurar session sem verificação SSL
        async with aiohttp.ClientSession() as session:
            print(f"Baixando: {url} -> {filename}")
            async with session.get(url, ssl=False) as response:
                if response.status == 200:
                    with open(output_path, 'wb') as f:
                        while True:
                            chunk = await response.content.read(8192)
                            if not chunk:
                                break
                            f.write(chunk)
                    print(f"Download concluído: {output_path}")
                    return output_path, None
                else:
                    error_msg = f"Erro ao baixar {url}: {response.status}"
                    print(error_msg)
                    return None, (original_filename, url, error_msg, response.status, page_num)
    except Exception as e:
        error_msg = f"Erro: {str(e)}"
        print(error_msg)
        return None, (original_filename, url, error_msg, str(e), page_num)

async def download_multipages(url, output_dir, original_filename, max_pages=50):
    """Tenta baixar múltiplas páginas de um documento"""
    pages = []
    errors = []
    
    # Verificar se há um parâmetro previewFileIndex na URL
    if 'previewFileIndex=' not in url:
        # Se não tiver, tenta fazer download como documento de página única
        result, error = await download_file(url, output_dir, original_filename)
        if result:
            pages.append(result)
        if error:
            errors.append(error)
        return pages, errors
    
    # Tenta baixar páginas sequenciais
    for page_num in range(1, max_pages + 1):
        # Atualizar o índice da página na URL
        page_url = re.sub(r'previewFileIndex=\d+', f'previewFileIndex={page_num}', url)
        
        # Tenta baixar a página
        result, error = await download_file(page_url, output_dir, original_filename, page_num)
        
        if result:
            pages.append(result)
        if error:
            errors.append(error)
            # Se recebermos um erro (especialmente 404), assumimos que chegamos ao fim do documento
            break
    
    print(f"Baixadas {len(pages)} páginas para {original_filename}")
    return pages, errors

def write_error_log(errors, log_file="error_log.txt"):
    """Escreve os erros em um arquivo de log"""
    if not errors:
        return
    
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file, 'w') as f:
        f.write(f"=== LOG DE ERROS GERADO EM {timestamp} ===\n\n")
        f.write(f"Total de erros: {len(errors)}\n\n")
        
        for i, (filename, url, error_msg, status, page_num) in enumerate(errors, 1):
            f.write(f"ERRO #{i}:\n")
            f.write(f"Arquivo: {filename}\n")
            f.write(f"URL: {url}\n")
            f.write(f"Página: {page_num}\n")
            f.write(f"Mensagem: {error_msg}\n")
            f.write(f"Status/Detalhe: {status}\n")
            f.write("-" * 60 + "\n\n")
    
    print(f"Log de erros gerado em {log_file}")

def cleanup_downloads(download_dir, keep_downloads=False):
    """Remove a pasta de downloads se a conversão for concluída com sucesso"""
    if not keep_downloads and os.path.exists(download_dir):
        try:
            shutil.rmtree(download_dir)
            print(f"Pasta de downloads removida: {download_dir}")
        except Exception as e:
            print(f"Erro ao remover pasta de downloads: {str(e)}")

async def process_csv(csv_path, output_dir, pdf_dir, error_log_file="error_log.txt", convert_to_pdf=True, 
                    base_url="https://www.saude.df.gov.br", test_mode=False, test_limit=5, max_pages=20,
                    keep_downloads=False):
    """Processa um CSV com URLs de preview, salvando como PNG e opcionalmente convertendo para PDF"""
    # Ler o CSV
    file_data = []
    try:
        with open(csv_path, 'r', encoding='utf-8') as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                if 'PREVIEW_URL' in row and row['PREVIEW_URL'] and 'FILENAME' in row:
                    preview_url = row['PREVIEW_URL']
                    filename = row['FILENAME']
                    full_url = urljoin(base_url, preview_url)
                    file_data.append((full_url, filename))
    except Exception as e:
        print(f"Erro ao ler o CSV: {str(e)}")
        return False
    
    if not file_data:
        print("Nenhuma URL encontrada no CSV")
        return False
    
    # Aplicar modo de teste se necessário
    if test_mode and test_limit > 0:
        original_count = len(file_data)
        file_data = file_data[:test_limit]
        print(f"MODO DE TESTE: Limitando a {test_limit} arquivos de {original_count} disponíveis")
    
    print(f"Processando {len(file_data)} registros do CSV")
    
    # Baixar as URLs (usando um número limitado de tarefas concorrentes)
    semaphore = asyncio.Semaphore(10)  # Limitar a 10 downloads simultâneos
    all_pages = {}  # Dicionário para armazenar páginas por documento
    all_errors = []
    
    async def process_document(url, filename):
        async with semaphore:
            # Tenta baixar todas as páginas do documento
            pages, errors = await download_multipages(url, output_dir, filename, max_pages)
            return filename, pages, errors
    
    # Criar tarefas para processar todos os documentos
    tasks = []
    for url, filename in file_data:
        task = asyncio.create_task(process_document(url, filename))
        tasks.append(task)
    
    # Aguardar todas as tarefas e coletar resultados
    results = await asyncio.gather(*tasks)
    
    # Processar resultados
    for filename, pages, errors in results:
        if pages:
            # Remover aspas do nome do arquivo, se houver
            clean_filename = filename.strip('"')
            base_name = os.path.splitext(clean_filename)[0]
            all_pages[base_name] = pages
        
        if errors:
            all_errors.extend(errors)
    
    # Gerar log de erros se houver
    if all_errors:
        write_error_log(all_errors, error_log_file)
    
    # Estatísticas de download
    success_docs = len(all_pages)
    total_pages = sum(len(pages) for pages in all_pages.values())
    print(f"Downloads concluídos: {success_docs}/{len(file_data)} documentos, {total_pages} páginas no total (Erros: {len(all_errors)})")
    
    # Variável para controlar o sucesso da conversão
    conversion_success = False
    
    # Converter para PDF
    if convert_to_pdf and all_pages:
        print(f"Iniciando conversão de {total_pages} páginas para PDF...")
        
        # Criar conversor
        converter = AsyncImageToPdfConverter(dpi=300)
        
        try:
            # Garantir que o diretório de PDF exista
            os.makedirs(pdf_dir, exist_ok=True)
            
            # Converter cada documento para um PDF
            conversion_results = []
            for base_name, pages in all_pages.items():
                if len(pages) == 0:
                    continue
                
                pdf_path = os.path.join(pdf_dir, f"{base_name}.pdf")
                
                if len(pages) == 1:
                    # Documento de página única
                    success = await converter.convert_single_image(pages[0], pdf_path)
                    conversion_results.append(success)
                    if success:
                        print(f"Documento convertido: {pdf_path}")
                    else:
                        print(f"Falha ao converter: {base_name}")
                else:
                    # Documento multipáginas
                    success = await converter.convert_multiple_images(pages, pdf_path)
                    conversion_results.append(success)
                    if success:
                        print(f"Documento multipáginas convertido ({len(pages)} páginas): {pdf_path}")
                    else:
                        print(f"Falha ao converter documento multipáginas: {base_name}")
            
            # Verificar se todas as conversões foram bem-sucedidas
            conversion_success = all(conversion_results) and len(conversion_results) > 0
            
            print("Conversão para PDF concluída")
                    
        except Exception as e:
            error_msg = f"Erro durante a conversão para PDF: {str(e)}"
            print(error_msg)
            print(traceback.format_exc())
            
            # Adicionar erro de conversão ao log
            with open(error_log_file, 'a') as f:
                f.write("\nERRO NA CONVERSÃO PARA PDF:\n")
                f.write(f"{error_msg}\n")
                f.write(traceback.format_exc())
        finally:
            # Liberar recursos
            converter.close()
    
    # Limpar pasta de downloads se a conversão foi bem-sucedida
    if conversion_success and not keep_downloads:
        cleanup_downloads(output_dir, keep_downloads)
    
    return success_docs > 0

async def main():
    """Função principal"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Download e conversão de arquivos para PNG e PDF')
    parser.add_argument('csv_file', help='Arquivo CSV com URLs de preview')
    parser.add_argument('--output-dir', '-o', default='downloads', help='Diretório para salvar os arquivos baixados')
    parser.add_argument('--pdf-dir', '-p', default='pdfs', help='Diretório para salvar os PDFs')
    parser.add_argument('--error-log', '-e', default='error_log.txt', help='Arquivo para registrar erros')
    parser.add_argument('--no-pdf', action='store_true', help='Não converter para PDF')
    parser.add_argument('--base-url', '-b', default='https://www.saude.df.gov.br', help='URL base')
    parser.add_argument('--test', '-t', action='store_true', help='Ativar modo de teste (baixa apenas alguns arquivos)')
    parser.add_argument('--limit', '-l', type=int, default=5, help='Número de arquivos a baixar no modo de teste')
    parser.add_argument('--max-pages', '-m', type=int, default=20, help='Número máximo de páginas a tentar por documento')
    parser.add_argument('--keep-downloads', '-k', action='store_true', help='Manter os arquivos PNG após a conversão')
    
    args = parser.parse_args()
    
    # Configurar logging
    setup_pdf_logging()
    
    # Processar o CSV
    success = await process_csv(
        args.csv_file, 
        args.output_dir, 
        args.pdf_dir,
        args.error_log,
        not args.no_pdf, 
        args.base_url,
        args.test,
        args.limit,
        args.max_pages,
        args.keep_downloads
    )
    
    return 0 if success else 1

if __name__ == "__main__":
    asyncio.run(main())