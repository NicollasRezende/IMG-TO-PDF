# utils/image_downloader.py

import os
import tempfile
import traceback
import logging
import asyncio
import aiohttp
from typing import Optional, Tuple, List, Dict, Set
from urllib.parse import urlparse
import time
from concurrent.futures import ThreadPoolExecutor

# Configuração do logger
logger = logging.getLogger(__name__)

def setup_logging(log_level: int = logging.INFO, log_file: Optional[str] = None) -> None:
    """
    Configura o sistema de logging.
    
    Args:
        log_level: Nível de logging (default: logging.INFO)
        log_file: Caminho para o arquivo de log (opcional)
    """
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    # Configuração básica do logging
    if log_file:
        logging.basicConfig(
            level=log_level,
            format=log_format,
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
    else:
        logging.basicConfig(
            level=log_level,
            format=log_format
        )
    
    logger.debug("Logging configurado com sucesso")

class AsyncImageDownloader:
    """Classe para download assíncrono de imagens a partir de URLs."""
    
    # Mapeamento de tipos de conteúdo para extensões de arquivo
    CONTENT_TYPE_MAP = {
        'image/jpeg': '.jpg',
        'image/png': '.png',
        'image/gif': '.gif',
        'image/webp': '.webp',
        'image/tiff': '.tiff',
        'image/bmp': '.bmp',
        'image/svg+xml': '.svg'
    }
    
    def __init__(self, output_dir: Optional[str] = None, 
                 timeout: int = 30, 
                 max_concurrent_downloads: int = 20,
                 chunk_size: int = 8192) -> None:
        """
        Inicializa o downloader de imagens assíncrono.
        
        Args:
            output_dir: Diretório padrão para salvar as imagens baixadas (opcional)
            timeout: Tempo limite padrão para requisições em segundos
            max_concurrent_downloads: Número máximo de downloads concorrentes
            chunk_size: Tamanho dos chunks para download em bytes
        """
        self.output_dir = output_dir
        self.timeout = timeout
        self.max_concurrent_downloads = max_concurrent_downloads
        self.chunk_size = chunk_size
        self.semaphore = None  # Será inicializado durante o download
        logger.debug(f"AsyncImageDownloader inicializado: output_dir={output_dir}, "
                    f"timeout={timeout}, max_concurrent={max_concurrent_downloads}")
    
    def _extract_filename_from_headers(self, headers: Dict) -> Optional[str]:
        """
        Extrai o nome do arquivo dos cabeçalhos HTTP.
        
        Args:
            headers: Cabeçalhos HTTP da resposta
            
        Returns:
            Nome do arquivo ou None se não encontrado
        """
        content_disposition = headers.get('Content-Disposition')
        if content_disposition:
            import re
            filename_match = re.search(r'filename="?([^";]+)"?', content_disposition)
            if filename_match:
                return filename_match.group(1)
        return None
    
    def _extract_filename_from_url(self, url: str) -> str:
        """
        Extrai o nome do arquivo da URL.
        
        Args:
            url: URL da imagem
            
        Returns:
            Nome do arquivo extraído da URL
        """
        parsed_url = urlparse(url)
        path = parsed_url.path
        filename = os.path.basename(path)
        
        # Remove parâmetros de consulta do nome do arquivo, se houver
        if '?' in filename:
            filename = filename.split('?')[0]
            
        return filename
    
    def _get_extension_from_content_type(self, content_type: str) -> str:
        """
        Determina a extensão do arquivo com base no tipo de conteúdo.
        
        Args:
            content_type: Tipo de conteúdo MIME
            
        Returns:
            Extensão do arquivo mais adequada
        """
        # Limpa o tipo de conteúdo (remove parâmetros)
        content_type = content_type.split(';')[0].strip().lower()
        
        # Retorna a extensão correspondente ou .img como fallback
        return self.CONTENT_TYPE_MAP.get(content_type, '.img')
    
    def _validate_image_content_type(self, content_type: str) -> bool:
        """
        Verifica se o tipo de conteúdo corresponde a uma imagem.
        
        Args:
            content_type: Tipo de conteúdo MIME
            
        Returns:
            True se for uma imagem, False caso contrário
        """
        content_type = content_type.split(';')[0].strip().lower()
        return content_type.startswith('image/') or content_type in self.CONTENT_TYPE_MAP
    
    async def _ensure_directory(self, directory: str) -> None:
        """
        Cria um diretório se não existir (thread-safe).
        
        Args:
            directory: Caminho do diretório a ser criado
        """
        if directory and not os.path.exists(directory):
            # Use run_in_executor para operações de I/O bloqueantes
            with ThreadPoolExecutor() as executor:
                await asyncio.get_event_loop().run_in_executor(
                    executor, lambda: os.makedirs(directory, exist_ok=True)
                )
            logger.debug(f"Diretório criado: {directory}")
    
    async def download(self, url: str, output_path: Optional[str] = None, 
                      output_dir: Optional[str] = None, timeout: Optional[int] = None) -> Optional[str]:
        """
        Baixa uma imagem a partir de uma URL de forma assíncrona.
        
        Args:
            url: URL da imagem a ser baixada
            output_path: Caminho completo onde a imagem será salva (opcional)
            output_dir: Diretório onde a imagem será salva (sobrepõe o diretório padrão)
            timeout: Tempo limite para a requisição em segundos (sobrepõe o timeout padrão)
            
        Returns:
            Caminho para a imagem baixada ou None se falhar
        """
        # Garante que o semáforo está inicializado
        if self.semaphore is None:
            self.semaphore = asyncio.Semaphore(self.max_concurrent_downloads)
        
        # Usa o semáforo para limitar o número de downloads concorrentes
        async with self.semaphore:
            timeout_value = timeout if timeout is not None else self.timeout
            
            logger.info(f"Iniciando download da imagem: {url} (timeout: {timeout_value}s)")
            
            try:
                # Configura timeout para aiohttp
                timeout_obj = aiohttp.ClientTimeout(total=timeout_value)
                
                async with aiohttp.ClientSession(timeout=timeout_obj) as session:
                    async with session.get(url) as response:
                        # Verifica se a requisição foi bem-sucedida
                        response.raise_for_status()
                        
                        # Verifica o tipo de conteúdo
                        content_type = response.headers.get('Content-Type', '')
                        if not self._validate_image_content_type(content_type):
                            logger.warning(f"O conteúdo da URL não parece ser uma imagem: {content_type}")
                        
                        # Se output_path for fornecido, usa-o diretamente
                        if output_path:
                            final_path = output_path
                        else:
                            # Determina o nome do arquivo
                            filename = self._extract_filename_from_headers(response.headers)
                            
                            if not filename:
                                filename = self._extract_filename_from_url(url)
                            
                            # Verifica se o nome do arquivo tem uma extensão
                            if not filename or '.' not in filename:
                                # Cria um nome de arquivo com base no hash da URL e uma extensão apropriada
                                ext = self._get_extension_from_content_type(content_type)
                                filename = f"downloaded_image_{abs(hash(url))}{ext}"
                            
                            # Define o diretório de saída
                            target_dir = output_dir or self.output_dir
                            
                            if target_dir:
                                await self._ensure_directory(target_dir)
                                final_path = os.path.join(target_dir, filename)
                            else:
                                # Usa um diretório temporário se nenhum foi especificado
                                temp_dir = tempfile.gettempdir()
                                final_path = os.path.join(temp_dir, filename)
                        
                        # Cria o diretório de destino se não existir
                        dest_dir = os.path.dirname(final_path)
                        if dest_dir:
                            await self._ensure_directory(dest_dir)
                        
                        # Salva a imagem
                        with open(final_path, 'wb') as f:
                            async for chunk in response.content.iter_chunked(self.chunk_size):
                                f.write(chunk)
                        
                        logger.info(f"Download concluído: {url} -> {final_path}")
                        return final_path
                
            except aiohttp.ClientError as e:
                logger.error(f"Erro na requisição HTTP: {str(e)}")
                return None
            except Exception as e:
                logger.error(f"Erro ao baixar a imagem: {str(e)}")
                logger.debug(traceback.format_exc())
                return None
    
    async def download_multiple(self, urls: List[str], output_dir: Optional[str] = None, 
                               timeout: Optional[int] = None, 
                               progress_callback: Optional[callable] = None) -> List[str]:
        """
        Baixa múltiplas imagens a partir de URLs de forma assíncrona.
        
        Args:
            urls: Lista de URLs das imagens a serem baixadas
            output_dir: Diretório onde as imagens serão salvas (sobrepõe o diretório padrão)
            timeout: Tempo limite para as requisições em segundos (sobrepõe o timeout padrão)
            progress_callback: Função de callback para reportar progresso (recebe n_concluídos, total)
            
        Returns:
            Lista dos caminhos para as imagens baixadas com sucesso
        """
        if not urls:
            return []
        
        # Inicializa o semáforo se necessário
        if self.semaphore is None:
            self.semaphore = asyncio.Semaphore(self.max_concurrent_downloads)
        
        # Prepara as tarefas de download
        tasks = []
        for url in urls:
            task = asyncio.create_task(self.download(url, output_dir=output_dir, timeout=timeout))
            tasks.append(task)
        
        # Monitora o progresso
        downloaded_paths = []
        for i, task_group in enumerate(asyncio.as_completed(tasks)):
            path = await task_group
            if path:
                downloaded_paths.append(path)
            
            # Atualiza o progresso, se um callback for fornecido
            if progress_callback:
                progress_callback(i + 1, len(urls))
        
        logger.info(f"Download múltiplo concluído: {len(downloaded_paths)}/{len(urls)} imagens baixadas")
        return downloaded_paths
    
    async def check_url(self, url: str, timeout: Optional[int] = None) -> Tuple[bool, str]:
        """
        Verifica se uma URL contém uma imagem válida de forma assíncrona.
        
        Args:
            url: URL a ser verificada
            timeout: Tempo limite para a requisição em segundos (sobrepõe o timeout padrão)
            
        Returns:
            Tupla (é_válido, tipo_de_conteúdo)
        """
        timeout_value = timeout if timeout is not None else self.timeout
        
        try:
            # Configura timeout para aiohttp
            timeout_obj = aiohttp.ClientTimeout(total=timeout_value)
            
            async with aiohttp.ClientSession(timeout=timeout_obj) as session:
                async with session.head(url) as response:
                    response.raise_for_status()
                    
                    content_type = response.headers.get('Content-Type', '')
                    is_valid = self._validate_image_content_type(content_type)
                    
                    return is_valid, content_type
        
        except Exception as e:
            logger.error(f"Erro ao verificar URL: {str(e)}")
            return False, str(e)

    async def batch_download(self, urls: List[str], output_dir: Optional[str] = None,
                           timeout: Optional[int] = None, batch_size: int = 100) -> List[str]:
        """
        Baixa grandes volumes de imagens usando processamento em lotes.
        
        Args:
            urls: Lista de URLs das imagens a serem baixadas
            output_dir: Diretório onde as imagens serão salvas
            timeout: Tempo limite para as requisições
            batch_size: Número de imagens por lote
            
        Returns:
            Lista de caminhos para as imagens baixadas com sucesso
        """
        if not urls:
            return []
        
        total_urls = len(urls)
        logger.info(f"Iniciando download em lotes de {total_urls} imagens (tamanho do lote: {batch_size})")
        
        start_time = time.time()
        all_downloaded = []
        batch_count = (total_urls + batch_size - 1) // batch_size  # Arredonda para cima
        
        # Define a função de callback para mostrar o progresso
        def progress_callback(completed: int, total: int, batch_index: int):
            overall_progress = (batch_index * batch_size + completed) / total_urls * 100
            logger.info(f"Progresso: {overall_progress:.1f}% ({batch_index * batch_size + completed}/{total_urls})")
        
        # Processa em lotes
        for i in range(batch_count):
            batch_start = i * batch_size
            batch_end = min(batch_start + batch_size, total_urls)
            batch_urls = urls[batch_start:batch_end]
            
            logger.info(f"Processando lote {i+1}/{batch_count} ({len(batch_urls)} URLs)")
            
            # Prepara um callback específico para este lote
            batch_callback = lambda completed, total: progress_callback(completed, total, i)
            
            # Processa o lote
            batch_results = await self.download_multiple(
                batch_urls, output_dir=output_dir, timeout=timeout, 
                progress_callback=batch_callback
            )
            
            all_downloaded.extend(batch_results)
            
            # Estatísticas do lote
            batch_success_rate = len(batch_results) / len(batch_urls) * 100
            logger.info(f"Lote {i+1} concluído. Taxa de sucesso: {batch_success_rate:.1f}%")
        
        # Estatísticas finais
        elapsed_time = time.time() - start_time
        success_rate = len(all_downloaded) / total_urls * 100
        avg_time_per_image = elapsed_time / total_urls if total_urls > 0 else 0
        
        logger.info(f"Download em lotes concluído. Tempo total: {elapsed_time:.1f}s")
        logger.info(f"Taxa de sucesso: {success_rate:.1f}% ({len(all_downloaded)}/{total_urls})")
        logger.info(f"Tempo médio por imagem: {avg_time_per_image:.3f}s")
        
        return all_downloaded


# Função auxiliar para uso direto via linha de comando
async def _async_main():
    """Função principal assíncrona para uso via linha de comando"""
    import argparse
    
    # Configuração dos argumentos de linha de comando
    parser = argparse.ArgumentParser(description='Downloader assíncrono de imagens a partir de URLs')
    
    # Argumentos gerais
    parser.add_argument('--log-level', type=str, choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        default='INFO', help='Nível de logging')
    parser.add_argument('--log-file', type=str, help='Arquivo para salvar os logs')
    parser.add_argument('--timeout', type=int, default=30, help='Tempo limite para requisições em segundos')
    parser.add_argument('--output-dir', '-o', type=str, help='Diretório para salvar as imagens')
    parser.add_argument('--concurrent', '-c', type=int, default=20, 
                        help='Número máximo de downloads concorrentes')
    
    # Subcomandos
    subparsers = parser.add_subparsers(dest='command', help='Comando a ser executado')
    
    # Subcomando para baixar uma única imagem
    single_parser = subparsers.add_parser('single', help='Baixar uma única imagem')
    single_parser.add_argument('url', type=str, help='URL da imagem')
    single_parser.add_argument('--output', '-o', type=str, help='Caminho de saída para a imagem')
    
    # Subcomando para baixar múltiplas imagens
    multi_parser = subparsers.add_parser('multi', help='Baixar múltiplas imagens')
    multi_parser.add_argument('urls', type=str, nargs='+', help='URLs das imagens')
    
    # Subcomando para baixar a partir de um arquivo
    file_parser = subparsers.add_parser('file', help='Baixar URLs de um arquivo de texto')
    file_parser.add_argument('file', type=str, help='Arquivo contendo URLs (uma por linha)')
    file_parser.add_argument('--batch-size', type=int, default=100, help='Tamanho do lote para processamento')
    
    # Subcomando para verificar se uma URL contém uma imagem válida
    check_parser = subparsers.add_parser('check', help='Verificar se uma URL contém uma imagem válida')
    check_parser.add_argument('url', type=str, help='URL a ser verificada')
    
    args = parser.parse_args()
    
    # Configuração do logging
    log_level = getattr(logging, args.log_level)
    setup_logging(log_level, args.log_file)
    
    # Inicializa o downloader
    downloader = AsyncImageDownloader(
        output_dir=args.output_dir, 
        timeout=args.timeout,
        max_concurrent_downloads=args.concurrent
    )
    
    # Executa o comando especificado
    if args.command == 'single':
        result = await downloader.download(args.url, output_path=args.output)
        if result:
            print(f"Imagem baixada com sucesso: {result}")
            return 0
        else:
            print("Falha ao baixar a imagem")
            return 1
    
    elif args.command == 'multi':
        results = await downloader.download_multiple(args.urls)
        if results:
            print(f"Imagens baixadas com sucesso ({len(results)}/{len(args.urls)}):")
            for path in results:
                print(f"  - {path}")
            return 0
        else:
            print("Falha ao baixar as imagens")
            return 1
    
    elif args.command == 'file':
        # Lê URLs de um arquivo
        try:
            with open(args.file, 'r') as f:
                urls = [line.strip() for line in f if line.strip()]
        except Exception as e:
            logger.error(f"Erro ao ler o arquivo: {str(e)}")
            return 1
        
        if not urls:
            print("Nenhuma URL encontrada no arquivo")
            return 1
        
        print(f"Lidas {len(urls)} URLs do arquivo")
        results = await downloader.batch_download(
            urls, 
            output_dir=args.output_dir, 
            timeout=args.timeout,
            batch_size=args.batch_size
        )
        
        success_rate = len(results) / len(urls) * 100
        print(f"Download concluído. Taxa de sucesso: {success_rate:.1f}% ({len(results)}/{len(urls)})")
        return 0
    
    elif args.command == 'check':
        is_valid, content_type = await downloader.check_url(args.url)
        if is_valid:
            print(f"URL válida. Tipo de conteúdo: {content_type}")
            return 0
        else:
            print(f"URL inválida. Resposta: {content_type}")
            return 1
    
    else:
        parser.print_help()
        return 0

def main():
    """Wrapper síncrono para a função principal assíncrona"""
    return asyncio.run(_async_main())

if __name__ == "__main__":
    import sys
    sys.exit(main())