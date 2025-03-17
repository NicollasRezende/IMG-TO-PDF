# utils/csv_url_processor.py

import os
import csv
import logging
import time
from typing import List, Dict, Optional, Tuple
import asyncio
from urllib.parse import urljoin
from utils.image_downloader import AsyncImageDownloader
from utils.image_to_pdf import AsyncImageToPdfConverter

logger = logging.getLogger(__name__)

class CSVPreviewURLProcessor:
    """Classe para processar URLs de preview de um arquivo CSV."""
    
    def __init__(self, base_url: str = "https://www.saude.df.gov.br") -> None:
        """
        Inicializa o processador de URLs.
        
        Args:
            base_url: URL base para completar os caminhos relativos
        """
        self.base_url = base_url
        logger.debug(f"CSVPreviewURLProcessor inicializado com base_url: {base_url}")
    
    def read_csv(self, file_path: str) -> List[Dict[str, str]]:
        """
        Lê o arquivo CSV e retorna seus dados como uma lista de dicionários.
        
        Args:
            file_path: Caminho para o arquivo CSV
            
        Returns:
            Lista de dicionários com os dados do CSV
        """
        if not os.path.exists(file_path):
            logger.error(f"Arquivo CSV não encontrado: {file_path}")
            return []
        
        try:
            with open(file_path, 'r', encoding='utf-8') as csv_file:
                # Usar o DictReader para obter os dados como dicionários
                reader = csv.DictReader(csv_file)
                
                # Verificar se as colunas necessárias existem
                required_columns = ['FILENAME', 'FILEENTRYID', 'PREVIEW_URL']
                first_row = next(reader, None)
                
                if first_row is None:
                    logger.error(f"CSV vazio: {file_path}")
                    return []
                
                # Verificar as colunas
                for column in required_columns:
                    if column not in first_row:
                        logger.error(f"Coluna obrigatória '{column}' não encontrada no CSV")
                        return []
                
                # Reiniciar o leitor
                csv_file.seek(0)
                next(reader)  # Pular o cabeçalho
                
                data = [row for row in reader]
                logger.info(f"Lidas {len(data)} linhas do arquivo CSV {file_path}")
                return data
                
        except Exception as e:
            logger.error(f"Erro ao ler o arquivo CSV {file_path}: {str(e)}", exc_info=True)
            return []
    
    def extract_preview_urls(self, csv_data: List[Dict[str, str]]) -> List[Tuple[str, str]]:
        """
        Extrai as URLs de preview do CSV e as completa com a URL base.
        
        Args:
            csv_data: Lista de dicionários com os dados do CSV
            
        Returns:
            Lista de tuplas (filename, url_completa)
        """
        if not csv_data:
            return []
        
        result = []
        for item in csv_data:
            filename = item.get('FILENAME', '').strip('"')
            preview_url = item.get('PREVIEW_URL', '')
            
            if preview_url:
                # Completar URL relativa com a URL base
                full_url = urljoin(self.base_url, preview_url)
                result.append((filename, full_url))
            
        logger.info(f"Extraídas {len(result)} URLs de preview")
        return result
    
    def write_urls_to_file(self, urls: List[Tuple[str, str]], output_file: str) -> bool:
        """
        Escreve as URLs extraídas em um arquivo de texto.
        
        Args:
            urls: Lista de tuplas (filename, url_completa)
            output_file: Caminho para o arquivo de saída
            
        Returns:
            True se a operação for bem-sucedida, False caso contrário
        """
        if not urls:
            logger.warning("Nenhuma URL para escrever no arquivo")
            return False
        
        try:
            # Garantir que o diretório exista
            output_dir = os.path.dirname(output_file)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir, exist_ok=True)
            
            with open(output_file, 'w', encoding='utf-8') as f:
                for _, url in urls:
                    f.write(f"{url}\n")
            
            logger.info(f"{len(urls)} URLs escritas no arquivo {output_file}")
            return True
        
        except Exception as e:
            logger.error(f"Erro ao escrever URLs no arquivo {output_file}: {str(e)}", exc_info=True)
            return False
    
    def write_urls_map_to_file(self, urls: List[Tuple[str, str]], output_file: str) -> bool:
        """
        Escreve o mapeamento de nomes de arquivo para URLs em um arquivo CSV.
        
        Args:
            urls: Lista de tuplas (filename, url_completa)
            output_file: Caminho para o arquivo de saída
            
        Returns:
            True se a operação for bem-sucedida, False caso contrário
        """
        if not urls:
            logger.warning("Nenhuma URL para escrever no arquivo de mapeamento")
            return False
        
        try:
            # Garantir que o diretório exista
            output_dir = os.path.dirname(output_file)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir, exist_ok=True)
            
            with open(output_file, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['FILENAME', 'FULL_URL'])
                for filename, url in urls:
                    writer.writerow([filename, url])
            
            logger.info(f"Mapeamento de {len(urls)} URLs escrito no arquivo {output_file}")
            return True
        
        except Exception as e:
            logger.error(f"Erro ao escrever mapeamento no arquivo {output_file}: {str(e)}", exc_info=True)
            return False
    
    def process_csv_file(self, csv_path: str, urls_output_file: str, map_output_file: Optional[str] = None) -> Tuple[bool, List[str]]:
        """
        Processa um arquivo CSV e extrai as URLs de preview.
        
        Args:
            csv_path: Caminho para o arquivo CSV
            urls_output_file: Arquivo de saída para a lista de URLs
            map_output_file: Arquivo de saída opcional para o mapeamento filename->URL
            
        Returns:
            Tupla (sucesso, lista_de_urls)
        """
        # Ler o CSV
        csv_data = self.read_csv(csv_path)
        if not csv_data:
            return False, []
        
        # Extrair as URLs
        url_tuples = self.extract_preview_urls(csv_data)
        if not url_tuples:
            return False, []
        
        # Escrever as URLs em um arquivo
        success = self.write_urls_to_file(url_tuples, urls_output_file)
        
        # Se especificado, escrever o mapeamento
        if map_output_file:
            self.write_urls_map_to_file(url_tuples, map_output_file)
        
        # Retornar apenas as URLs (sem os filenames)
        urls = [url for _, url in url_tuples]
        return success, urls

# Função de utilidade para uso direto
async def process_csv_and_download(csv_path: str, 
                                 output_dir: str,
                                 downloader: 'AsyncImageDownloader',
                                 converter: 'AsyncImageToPdfConverter',
                                 base_url: str = "https://www.saude.df.gov.br",
                                 combine: bool = False,
                                 batch_size: int = 100,
                                 verify_ssl: bool = True) -> bool:
    """
    Processa um arquivo CSV, extrai URLs de preview, baixa e converte imagens.
    
    Args:
        csv_path: Caminho para o arquivo CSV
        output_dir: Diretório de saída para os arquivos
        downloader: Instância do downloader de imagens
        converter: Instância do conversor de PDF
        base_url: URL base para caminhos relativos
        combine: Se deve combinar todas as imagens em um único PDF
        batch_size: Tamanho do lote para processamento
        
    Returns:
        True se a operação for bem-sucedida, False caso contrário
    """
    logger = logging.getLogger("csv_processor")
    
    try:
        # Definir caminhos de saída
        urls_dir = os.path.join(output_dir, "urls")
        imgs_dir = os.path.join(output_dir, "imgs")
        pdfs_dir = os.path.join(output_dir, "pdfs")
        
        # Garantir que os diretórios existam
        for directory in [urls_dir, imgs_dir, pdfs_dir]:
            os.makedirs(directory, exist_ok=True)
        
        # Definir nomes de arquivos
        urls_file = os.path.join(urls_dir, "preview_urls.txt")
        map_file = os.path.join(urls_dir, "filename_url_map.csv")
        
        # Processar o CSV
        processor = CSVPreviewURLProcessor(base_url=base_url)
        success, urls = processor.process_csv_file(csv_path, urls_file, map_file)
        
        if not success or not urls:
            logger.error("Falha ao processar o arquivo CSV ou nenhuma URL extraída")
            return False
        
        # Baixar as imagens
        logger.info(f"Iniciando download de {len(urls)} arquivos...")
        start_time = time.time()
        
        downloaded_files = await downloader.batch_download(
            urls, 
            output_dir=imgs_dir, 
            batch_size=batch_size,
            verify_ssl=verify_ssl
        )
        
        if not downloaded_files:
            logger.error("Nenhum arquivo baixado com sucesso")
            return False
        
        download_time = time.time() - start_time
        logger.info(f"Download concluído em {download_time:.1f}s. "
                   f"Taxa de sucesso: {len(downloaded_files)/len(urls)*100:.1f}% "
                   f"({len(downloaded_files)}/{len(urls)})")
        
        # Filtrar apenas as imagens (remover arquivos PDF)
        image_files = [file for file in downloaded_files 
                      if not file.lower().endswith('.pdf')]
        
        if not image_files:
            logger.warning("Nenhuma imagem para converter. Todos os arquivos baixados são PDFs ou outros formatos.")
            return True  # Retornamos True porque o download foi bem-sucedido
        
        # Converter as imagens baixadas para PDF
        logger.info(f"Iniciando conversão de {len(image_files)} imagens para PDF...")
        
        if combine and image_files:
            # Combina todas as imagens em um único PDF
            pdf_path = os.path.join(pdfs_dir, f"combined_{int(time.time())}.pdf")
            success = await converter.convert_multiple_images(image_files, pdf_path)
            
            if success:
                logger.info(f"Conversão concluída com sucesso. PDF salvo em: {pdf_path}")
            else:
                logger.error("Falha ao combinar as imagens em um único PDF")
                return False
        elif image_files:
            # Converte cada imagem individualmente
            success = await converter.batch_convert(
                image_files, imgs_dir, pdfs_dir, batch_size=batch_size
            )
            
            if success:
                logger.info("Conversão individual das imagens concluída com sucesso")
            else:
                logger.error("Falha ao converter as imagens individualmente")
                return False
        
        total_time = time.time() - start_time
        logger.info(f"Processamento completo em {total_time:.1f}s")
        return True
    
    except Exception as e:
        logger.error(f"Erro durante o processamento do CSV e download: {str(e)}", exc_info=True)
        return False