# main.py
import asyncio
import logging
import os
import argparse
import time
from typing import List, Optional

# Importar os módulos utils
from utils.image_to_pdf import AsyncImageToPdfConverter, setup_logging as setup_pdf_logging
from utils.image_downloader import AsyncImageDownloader, setup_logging as setup_downloader_logging

async def process_urls_file(file_path: str, downloader: AsyncImageDownloader, 
                           converter: AsyncImageToPdfConverter, 
                           output_dir: str, combine: bool = False,
                           batch_size: int = 100) -> bool:
    """
    Processa um arquivo com lista de URLs para download e conversão.
    
    Args:
        file_path: Caminho para o arquivo com as URLs (uma por linha)
        downloader: Instância do downloader de imagens
        converter: Instância do conversor de PDF
        output_dir: Diretório de saída para os PDFs
        combine: Se deve combinar todas as imagens em um único PDF
        batch_size: Tamanho do lote para processamento
        
    Returns:
        True se a operação for bem-sucedida, False caso contrário
    """
    logger = logging.getLogger("main")
    
    try:
        # Lê as URLs do arquivo
        with open(file_path, 'r') as f:
            urls = [line.strip() for line in f if line.strip()]
        
        if not urls:
            logger.error(f"Nenhuma URL encontrada no arquivo {file_path}")
            return False
        
        logger.info(f"Lidas {len(urls)} URLs do arquivo {file_path}")
        
        # Define os diretórios de trabalho
        imgs_dir = os.path.join(output_dir, "imgs")
        pdfs_dir = os.path.join(output_dir, "pdfs")
        
        # Garante que os diretórios existam
        os.makedirs(imgs_dir, exist_ok=True)
        os.makedirs(pdfs_dir, exist_ok=True)
        
        # Baixa as imagens em lotes
        start_time = time.time()
        logger.info(f"Iniciando download de {len(urls)} imagens...")
        
        downloaded_images = await downloader.batch_download(urls, output_dir=imgs_dir, batch_size=batch_size)
        
        if not downloaded_images:
            logger.error("Nenhuma imagem baixada com sucesso")
            return False
        
        download_time = time.time() - start_time
        logger.info(f"Download concluído em {download_time:.1f}s. "
                   f"Taxa de sucesso: {len(downloaded_images)/len(urls)*100:.1f}% "
                   f"({len(downloaded_images)}/{len(urls)})")
        
        # Converte as imagens baixadas para PDF
        logger.info(f"Iniciando conversão de {len(downloaded_images)} imagens para PDF...")
        
        if combine:
            # Combina todas as imagens em um único PDF
            pdf_path = os.path.join(pdfs_dir, f"combined_{int(time.time())}.pdf")
            success = await converter.convert_multiple_images(downloaded_images, pdf_path)
            
            if success:
                logger.info(f"Conversão concluída com sucesso. PDF salvo em: {pdf_path}")
            else:
                logger.error("Falha ao combinar as imagens em um único PDF")
                return False
        else:
            # Converte cada imagem individualmente
            success = await converter.batch_convert(
                downloaded_images, imgs_dir, pdfs_dir, batch_size=batch_size
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
        logger.error(f"Erro ao processar o arquivo de URLs: {str(e)}", exc_info=True)
        return False

async def main_async():
    """Função principal assíncrona."""
    # Configuração dos argumentos de linha de comando
    parser = argparse.ArgumentParser(description='Download e conversão de imagens para PDF')
    
    # Argumentos gerais
    parser.add_argument('--log-level', type=str, choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        default='INFO', help='Nível de logging')
    parser.add_argument('--log-file', type=str, default="logs/image_processor.log", 
                        help='Arquivo para salvar os logs')
    parser.add_argument('--output-dir', '-o', type=str, default="output",
                        help='Diretório base para saída')
    
    # Configurações do downloader
    parser.add_argument('--dl-timeout', type=int, default=30,
                        help='Tempo limite para requisições em segundos')
    parser.add_argument('--dl-concurrent', type=int, default=20,
                        help='Número máximo de downloads concorrentes')
    
    # Configurações do conversor
    parser.add_argument('--dpi', type=int, default=300,
                        help='Resolução DPI para a conversão')
    parser.add_argument('--workers', type=int, default=8,
                        help='Número máximo de workers para processamento')
    
    # Subcomandos
    subparsers = parser.add_subparsers(dest='command', help='Comando a ser executado')
    
    # Subcomando para processar uma única URL
    single_parser = subparsers.add_parser('single', help='Baixar e converter uma única imagem')
    single_parser.add_argument('url', type=str, help='URL da imagem')
    single_parser.add_argument('--output', type=str, help='Nome do arquivo PDF de saída')
    
    # Subcomando para processar várias URLs fornecidas na linha de comando
    multi_parser = subparsers.add_parser('multi', help='Baixar e converter múltiplas imagens')
    multi_parser.add_argument('urls', type=str, nargs='+', help='URLs das imagens')
    multi_parser.add_argument('--combine', '-c', action='store_true',
                             help='Combinar em um único PDF')
    
    # Subcomando para processar URLs de um arquivo
    file_parser = subparsers.add_parser('file', help='Processar URLs de um arquivo de texto')
    file_parser.add_argument('file_path', type=str, help='Caminho para o arquivo com as URLs')
    file_parser.add_argument('--combine', '-c', action='store_true',
                            help='Combinar em um único PDF')
    file_parser.add_argument('--batch-size', '-b', type=int, default=100,
                            help='Tamanho do lote para processamento')
    
    args = parser.parse_args()
    
    # Configurar logging
    log_dir = os.path.dirname(args.log_file)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)
        
    log_level = getattr(logging, args.log_level)
    setup_pdf_logging(log_level=log_level, log_file=args.log_file)
    setup_downloader_logging(log_level=log_level, log_file=args.log_file)
    
    # Configurar logger principal
    logger = logging.getLogger("main")
    logger.setLevel(log_level)
    if not logger.handlers:
        if args.log_file:
            handler = logging.FileHandler(args.log_file)
        else:
            handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
        logger.addHandler(handler)
    
    # Garantir que o diretório de saída exista
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)
    
    # Criar instâncias
    downloader = AsyncImageDownloader(
        timeout=args.dl_timeout,
        max_concurrent_downloads=args.dl_concurrent
    )
    
    converter = AsyncImageToPdfConverter(
        dpi=args.dpi,
        max_workers=args.workers
    )
    
    try:
        # Executar o comando selecionado
        if args.command == 'single':
            # Definir caminhos
            imgs_dir = os.path.join(args.output_dir, "imgs")
            pdfs_dir = os.path.join(args.output_dir, "pdfs")
            os.makedirs(imgs_dir, exist_ok=True)
            os.makedirs(pdfs_dir, exist_ok=True)
            
            # Download da imagem
            logger.info(f"Iniciando download de {args.url}")
            image_path = await downloader.download(args.url, output_dir=imgs_dir)
            
            if not image_path:
                logger.error(f"Falha ao baixar a imagem de {args.url}")
                return 1
            
            # Conversão para PDF
            if args.output:
                pdf_path = os.path.join(pdfs_dir, args.output)
            else:
                pdf_name = os.path.basename(os.path.splitext(image_path)[0]) + ".pdf"
                pdf_path = os.path.join(pdfs_dir, pdf_name)
            
            logger.info(f"Convertendo imagem para PDF: {pdf_path}")
            success = await converter.convert_single_image(image_path, pdf_path)
            
            if success:
                logger.info(f"Processamento concluído com sucesso: {pdf_path}")
                return 0
            else:
                logger.error("Falha ao converter a imagem para PDF")
                return 1
            
        elif args.command == 'multi':
            # Definir caminhos
            imgs_dir = os.path.join(args.output_dir, "imgs")
            pdfs_dir = os.path.join(args.output_dir, "pdfs")
            os.makedirs(imgs_dir, exist_ok=True)
            os.makedirs(pdfs_dir, exist_ok=True)
            
            # Download das imagens
            logger.info(f"Iniciando download de {len(args.urls)} imagens")
            image_paths = await downloader.download_multiple(args.urls, output_dir=imgs_dir)
            
            if not image_paths:
                logger.error("Falha ao baixar as imagens")
                return 1
            
            logger.info(f"Baixadas {len(image_paths)} imagens com sucesso")
            
            # Conversão para PDF
            if args.combine:
                pdf_path = os.path.join(pdfs_dir, f"combined_{int(time.time())}.pdf")
                logger.info(f"Combinando imagens em um único PDF: {pdf_path}")
                success = await converter.convert_multiple_images(image_paths, pdf_path)
                
                if success:
                    logger.info(f"Combinação concluída com sucesso: {pdf_path}")
                else:
                    logger.error("Falha ao combinar as imagens em um único PDF")
                    return 1
            else:
                logger.info("Convertendo imagens individualmente")
                success = await converter.batch_convert(image_paths, imgs_dir, pdfs_dir)
                
                if success:
                    logger.info("Conversão individual concluída com sucesso")
                else:
                    logger.error("Falha ao converter as imagens individualmente")
                    return 1
            
            return 0
            
        elif args.command == 'file':
            success = await process_urls_file(
                args.file_path,
                downloader,
                converter,
                args.output_dir,
                args.combine,
                args.batch_size
            )
            
            return 0 if success else 1
            
        else:
            parser.print_help()
            return 0
            
    except Exception as e:
        logger.error(f"Erro não tratado: {str(e)}", exc_info=True)
        return 1
        
    finally:
        # Liberar recursos
        converter.close()
        
def main():
    """Função principal para uso como script."""
    return asyncio.run(main_async())

if __name__ == "__main__":
    import sys
    sys.exit(main())