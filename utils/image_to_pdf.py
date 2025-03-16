# utils/image_to_pdf.py

from PIL import Image
import os
import sys
import logging
import asyncio
from typing import List, Optional, Union, Tuple, Dict, Set
import traceback
import time
from concurrent.futures import ThreadPoolExecutor

# Configuração do logging
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

class AsyncImageToPdfConverter:
    """Classe para converter imagens para PDF com processamento assíncrono."""
    
    SUPPORTED_FORMATS = {'.png', '.jpg', '.jpeg', '.tiff', '.bmp', '.webp'}
    
    def __init__(self, dpi: int = 200, max_workers: int = 8) -> None:
        """
        Inicializa o conversor assíncrono.
        
        Args:
            dpi: Resolução para a conversão (default: 200)
            max_workers: Número máximo de threads para processamento paralelo
        """
        self.dpi = dpi
        self.max_workers = max_workers
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.semaphore = None  # Será inicializado durante a conversão
        logger.debug(f"AsyncImageToPdfConverter inicializado com DPI: {self.dpi}, max_workers: {max_workers}")

    def _validate_image_path(self, image_path: str) -> Tuple[bool, str]:
        """
        Valida se o caminho da imagem existe e é de um formato suportado.
        
        Args:
            image_path: Caminho para a imagem
            
        Returns:
            Tupla (é_válido, mensagem_de_erro)
        """
        if not os.path.exists(image_path):
            return False, f"O arquivo {image_path} não existe"
        
        ext = os.path.splitext(image_path)[1].lower()
        if ext not in self.SUPPORTED_FORMATS:
            return False, f"Formato não suportado: {ext}. Formatos suportados: {', '.join(self.SUPPORTED_FORMATS)}"
        
        return True, ""
    
    def _prepare_image(self, image_path: str) -> Optional[Image.Image]:
        """
        Prepara uma imagem para conversão.
        
        Args:
            image_path: Caminho para a imagem
            
        Returns:
            Objeto Image da PIL ou None se falhar
        """
        try:
            # Valida o caminho da imagem
            valid, error_msg = self._validate_image_path(image_path)
            if not valid:
                logger.error(error_msg)
                return None
            
            # Abre a imagem
            image = Image.open(image_path)
            
            # Converte para RGB se necessário
            if image.mode != 'RGB':
                image = image.convert('RGB')
                logger.debug(f"Imagem {image_path} convertida para RGB")
            
            return image
        
        except Exception as e:
            logger.error(f"Erro ao preparar a imagem {image_path}: {str(e)}")
            logger.debug(traceback.format_exc())
            return None
    
    async def _ensure_directory(self, directory: str) -> None:
        """
        Cria um diretório se não existir de forma assíncrona.
        
        Args:
            directory: Caminho do diretório a ser criado
        """
        if directory and not os.path.exists(directory):
            # Use run_in_executor para operações de I/O bloqueantes
            await asyncio.get_event_loop().run_in_executor(
                self.executor, lambda: os.makedirs(directory, exist_ok=True)
            )
            logger.debug(f"Diretório criado: {directory}")
    
    async def convert_single_image(self, image_path: str, output_path: Optional[str] = None) -> bool:
        """
        Converte uma única imagem para PDF de forma assíncrona.
        
        Args:
            image_path: Caminho para a imagem de entrada
            output_path: Caminho para o arquivo PDF de saída (opcional)
            
        Returns:
            True se a conversão for bem-sucedida, False caso contrário
        """
        logger.info(f"Iniciando conversão de {image_path} para PDF")
        
        try:
            # Define o caminho de saída se não fornecido
            if output_path is None:
                output_path = os.path.splitext(image_path)[0] + '.pdf'
            
            # Cria o diretório de saída se não existir
            output_dir = os.path.dirname(output_path)
            if output_dir:
                await self._ensure_directory(output_dir)
            
            # Executa a preparação e conversão da imagem em uma thread separada
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                self.executor,
                self._convert_image_sync,
                image_path,
                output_path
            )
            
            return result
        
        except Exception as e:
            logger.error(f"Erro durante a conversão assíncrona de {image_path}: {str(e)}")
            logger.debug(traceback.format_exc())
            return False
    
    def _convert_image_sync(self, image_path: str, output_path: str) -> bool:
        """
        Versão síncrona da conversão para ser executada em uma thread separada.
        
        Args:
            image_path: Caminho para a imagem de entrada
            output_path: Caminho para o arquivo PDF de saída
            
        Returns:
            True se a conversão for bem-sucedida, False caso contrário
        """
        try:
            # Prepara a imagem
            image = self._prepare_image(image_path)
            if image is None:
                return False
            
            # Salva como PDF
            image.save(output_path, 'PDF', resolution=self.dpi)
            logger.info(f"Conversão concluída: {image_path} -> {output_path}")
            return True
        
        except Exception as e:
            logger.error(f"Erro durante a conversão de {image_path}: {str(e)}")
            logger.debug(traceback.format_exc())
            return False
    
    async def convert_multiple_images(self, image_paths: List[str], output_path: str) -> bool:
        """
        Converte múltiplas imagens para um único arquivo PDF de forma assíncrona.
        
        Args:
            image_paths: Lista de caminhos das imagens
            output_path: Caminho para o arquivo PDF de saída
            
        Returns:
            True se a conversão for bem-sucedida, False caso contrário
        """
        if not image_paths:
            logger.error("Nenhuma imagem fornecida para conversão")
            return False
        
        logger.info(f"Iniciando conversão de {len(image_paths)} imagens para um único PDF")
        
        try:
            # Cria o diretório de saída se não existir
            output_dir = os.path.dirname(output_path)
            if output_dir:
                await self._ensure_directory(output_dir)
            
            # Executa a conversão múltipla em uma thread separada
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                self.executor,
                self._convert_multiple_sync,
                image_paths,
                output_path
            )
            
            return result
        
        except Exception as e:
            logger.error(f"Erro durante a conversão múltipla assíncrona: {str(e)}")
            logger.debug(traceback.format_exc())
            return False
    
    def _convert_multiple_sync(self, image_paths: List[str], output_path: str) -> bool:
        """
        Versão síncrona da conversão múltipla para ser executada em uma thread separada.
        
        Args:
            image_paths: Lista de caminhos das imagens
            output_path: Caminho para o arquivo PDF de saída
            
        Returns:
            True se a conversão for bem-sucedida, False caso contrário
        """
        try:
            # Prepara todas as imagens
            images = []
            for image_path in image_paths:
                image = self._prepare_image(image_path)
                if image is not None:
                    images.append(image)
                    logger.debug(f"Imagem adicionada: {image_path}")
            
            if not images:
                logger.error("Nenhuma imagem válida para processar")
                return False
            
            # A primeira imagem é a base, as outras são adicionadas como páginas
            first_image = images[0]
            remaining_images = images[1:]
            
            # Salva o PDF
            first_image.save(
                output_path, 
                'PDF', 
                resolution=self.dpi, 
                save_all=True, 
                append_images=remaining_images
            )
            
            logger.info(f"Conversão múltipla concluída: {len(images)} imagens -> {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Erro durante a conversão múltipla: {str(e)}")
            logger.debug(traceback.format_exc())
            return False
    
    async def convert_directory(self, input_dir: str, output_dir: Optional[str] = None, 
                               recursive: bool = False, combine: bool = False,
                               batch_size: int = 50) -> bool:
        """
        Converte todas as imagens em um diretório de forma assíncrona.
        
        Args:
            input_dir: Diretório de entrada com as imagens
            output_dir: Diretório de saída para os PDFs (opcional)
            recursive: Se deve processar subdiretórios
            combine: Se deve combinar todas as imagens em um único PDF
            batch_size: Tamanho do lote para processamento
            
        Returns:
            True se a conversão for bem-sucedida, False caso contrário
        """
        if not os.path.isdir(input_dir):
            logger.error(f"O diretório {input_dir} não existe")
            return False
        
        # Define o diretório de saída se não fornecido
        if output_dir is None:
            output_dir = input_dir
        
        # Cria o diretório de saída se não existir
        await self._ensure_directory(output_dir)
        
        # Encontra todas as imagens no diretório (executado em uma thread separada)
        loop = asyncio.get_event_loop()
        image_paths = await loop.run_in_executor(
            self.executor,
            self._find_images_in_directory,
            input_dir,
            recursive
        )
        
        if not image_paths:
            logger.warning(f"Nenhuma imagem encontrada no diretório {input_dir}")
            return False
        
        logger.info(f"Encontradas {len(image_paths)} imagens para conversão")
        
        # Combina todas as imagens em um único PDF ou converte individualmente
        if combine:
            # Nome do arquivo combinado baseado no nome do diretório
            dir_name = os.path.basename(os.path.normpath(input_dir))
            combined_pdf = os.path.join(output_dir, f"{dir_name}_combined.pdf")
            return await self.convert_multiple_images(image_paths, combined_pdf)
        else:
            # Converte cada imagem individualmente em lotes
            return await self.batch_convert(image_paths, input_dir, output_dir, batch_size)
    
    def _find_images_in_directory(self, input_dir: str, recursive: bool) -> List[str]:
        """
        Encontra todas as imagens no diretório.
        
        Args:
            input_dir: Diretório de entrada
            recursive: Se deve processar subdiretórios
            
        Returns:
            Lista de caminhos das imagens encontradas
        """
        image_paths = []
        
        if recursive:
            for root, _, files in os.walk(input_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    ext = os.path.splitext(file)[1].lower()
                    if ext in self.SUPPORTED_FORMATS:
                        image_paths.append(file_path)
        else:
            for file in os.listdir(input_dir):
                file_path = os.path.join(input_dir, file)
                if os.path.isfile(file_path):
                    ext = os.path.splitext(file)[1].lower()
                    if ext in self.SUPPORTED_FORMATS:
                        image_paths.append(file_path)
        
        return image_paths
    
    async def batch_convert(self, image_paths: List[str], input_dir: str, output_dir: str, 
                           batch_size: int = 50) -> bool:
        """
        Converte imagens em lotes para melhor desempenho.
        
        Args:
            image_paths: Lista de caminhos das imagens
            input_dir: Diretório de entrada
            output_dir: Diretório de saída
            batch_size: Tamanho do lote para processamento
            
        Returns:
            True se a conversão for bem-sucedida (pelo menos uma imagem), False caso contrário
        """
        if not image_paths:
            return False
        
        # Inicializa o semáforo se necessário
        if self.semaphore is None:
            self.semaphore = asyncio.Semaphore(self.max_workers)
        
        total_images = len(image_paths)
        start_time = time.time()
        success_count = 0
        
        logger.info(f"Iniciando conversão em lotes de {total_images} imagens (tamanho do lote: {batch_size})")
        
        # Divide as imagens em lotes
        batches = [image_paths[i:i + batch_size] for i in range(0, len(image_paths), batch_size)]
        
        for batch_idx, batch in enumerate(batches):
            logger.info(f"Processando lote {batch_idx+1}/{len(batches)} ({len(batch)} imagens)")
            
            # Prepara as tarefas de conversão para este lote
            tasks = []
            for image_path in batch:
                # Determina o caminho de saída relativo
                rel_path = os.path.relpath(image_path, input_dir)
                output_path = os.path.join(output_dir, os.path.splitext(rel_path)[0] + '.pdf')
                
                # Cria a tarefa de conversão
                task = asyncio.create_task(self.convert_single_image(image_path, output_path))
                tasks.append(task)
            
            # Executa todas as tarefas do lote
            batch_results = await asyncio.gather(*tasks)
            
            # Conta os sucessos neste lote
            batch_success = batch_results.count(True)
            success_count += batch_success
            
            # Estatísticas do lote
            batch_success_rate = batch_success / len(batch) * 100
            logger.info(f"Lote {batch_idx+1} concluído. Taxa de sucesso: {batch_success_rate:.1f}%")
            
            # Progresso geral
            progress = (batch_idx + 1) * batch_size / total_images * 100
            logger.info(f"Progresso geral: {min(progress, 100):.1f}%")
        
        # Estatísticas finais
        elapsed_time = time.time() - start_time
        success_rate = success_count / total_images * 100
        avg_time_per_image = elapsed_time / total_images if total_images > 0 else 0
        
        logger.info(f"Conversão em lotes concluída. Tempo total: {elapsed_time:.1f}s")
        logger.info(f"Taxa de sucesso: {success_rate:.1f}% ({success_count}/{total_images})")
        logger.info(f"Tempo médio por imagem: {avg_time_per_image:.3f}s")
        
        return success_count > 0
    
    def close(self):
        """Libera recursos alocados pelo conversor."""
        if self.executor:
            self.executor.shutdown(wait=True)
            logger.debug("Executor encerrado")


# Função auxiliar para uso direto via linha de comando
async def _async_main():
    """Função principal assíncrona para uso via linha de comando"""
    import argparse
    
    # Configuração dos argumentos de linha de comando
    parser = argparse.ArgumentParser(description='Conversor assíncrono de imagens para PDF')
    
    # Argumentos gerais
    parser.add_argument('--log-level', type=str, choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        default='INFO', help='Nível de logging')
    parser.add_argument('--log-file', type=str, help='Arquivo para salvar os logs')
    parser.add_argument('--dpi', type=int, default=200, help='Resolução DPI para a conversão')
    parser.add_argument('--workers', type=int, default=8, help='Número máximo de workers para processamento')
    
    # Subcomandos
    subparsers = parser.add_subparsers(dest='command', help='Comando a ser executado')
    
    # Subcomando para converter uma única imagem
    single_parser = subparsers.add_parser('single', help='Converter uma única imagem')
    single_parser.add_argument('image_path', type=str, help='Caminho da imagem')
    single_parser.add_argument('--output', '-o', type=str, help='Caminho do PDF de saída')
    
    # Subcomando para converter múltiplas imagens
    multi_parser = subparsers.add_parser('multi', help='Converter múltiplas imagens para um único PDF')
    multi_parser.add_argument('image_paths', type=str, nargs='+', help='Caminhos das imagens')
    multi_parser.add_argument('--output', '-o', type=str, required=True, help='Caminho do PDF de saída')
    
    # Subcomando para converter um diretório
    dir_parser = subparsers.add_parser('dir', help='Converter todas as imagens em um diretório')
    dir_parser.add_argument('input_dir', type=str, help='Diretório de entrada com as imagens')
    dir_parser.add_argument('--output-dir', '-o', type=str, help='Diretório de saída para os PDFs')
    dir_parser.add_argument('--recursive', '-r', action='store_true', help='Processar subdiretórios')
    dir_parser.add_argument('--combine', '-c', action='store_true', help='Combinar em um único PDF')
    dir_parser.add_argument('--batch-size', '-b', type=int, default=50, help='Tamanho do lote para processamento')
    
    args = parser.parse_args()
    
    # Configuração do logging
    log_level = getattr(logging, args.log_level)
    setup_logging(log_level, args.log_file)
    
    # Inicializa o conversor
    converter = AsyncImageToPdfConverter(dpi=args.dpi, max_workers=args.workers)
    
    try:
        # Executa o comando especificado
        if args.command == 'single':
            success = await converter.convert_single_image(args.image_path, args.output)
            if success:
                logger.info("Operação concluída com sucesso")
            else:
                logger.error("Operação concluída com erros")
                return 1
            
        elif args.command == 'multi':
            success = await converter.convert_multiple_images(args.image_paths, args.output)
            if success:
                logger.info("Operação concluída com sucesso")
            else:
                logger.error("Operação concluída com erros")
                return 1
            
        elif args.command == 'dir':
            success = await converter.convert_directory(
                args.input_dir, 
                args.output_dir, 
                args.recursive, 
                args.combine,
                args.batch_size
            )
            if success:
                logger.info("Operação concluída com sucesso")
            else:
                logger.error("Operação concluída com erros")
                return 1
            
        else:
            parser.print_help()
            
    finally:
        # Libera recursos
        converter.close()
    
    return 0

def main():
    """Wrapper síncrono para a função principal assíncrona"""
    return asyncio.run(_async_main())

if __name__ == "__main__":
    import sys
    sys.exit(main())