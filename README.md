# Image Downloader & PDF Converter

Uma aplicação robusta e assíncrona para baixar imagens da web e convertê-las para PDF com suporte para processamento em massa.

## Características Principais

- **Download assíncrono** de múltiplas imagens simultaneamente
- **Conversão otimizada** de imagens para PDF
- **Processamento em lotes** para lidar com grandes volumes (até 15k+ arquivos)
- **Interface de linha de comando** completa
- **Logging detalhado** com estatísticas de processamento
- **Tratamento robusto de erros**

## Requisitos

- Python 3.7+
- Dependências:
  - Pillow (PIL Fork)
  - aiohttp
  - requests
  - asyncio

## Instalação

1. Clone o repositório:
   ```bash
   git clone https://github.com/seu-usuario/image-downloader-pdf-converter.git
   cd image-downloader-pdf-converter
   ```

2. Crie um ambiente virtual (opcional, mas recomendado):
   ```bash
   python -m venv venv
   source venv/bin/activate  # No Windows: venv\Scripts\activate
   ```

3. Instale as dependências:
   ```bash
   pip install -r requirements.txt
   ```

## Estrutura do Projeto

```
image-downloader-pdf-converter/
├── main.py                       # Ponto de entrada principal
├── requirements.txt              # Dependências do projeto
├── utils/                        # Utilitários
│   ├── __init__.py
│   ├── image_downloader.py       # Módulo para download de imagens
│   └── image_to_pdf.py           # Módulo para conversão de imagens para PDF
├── logs/                         # Logs do sistema (criado automaticamente)
├── output/                       # Arquivos de saída (criado automaticamente)
│   ├── imgs/                     # Imagens baixadas
│   └── pdfs/                     # PDFs gerados
└── README.md
```

## Uso

### Processar uma única imagem

```bash
python main.py single https://example.com/image.jpg
```

### Processar múltiplas imagens

```bash
python main.py multi https://example.com/image1.jpg https://example.com/image2.jpg
```

### Combinar múltiplas imagens em um único PDF

```bash
python main.py multi https://example.com/image1.jpg https://example.com/image2.jpg --combine
```

### Processar URLs de um arquivo (ideal para processamento em massa)

```bash
python main.py file urls.txt --batch-size 200
```

### Personalizar a configuração

```bash
python main.py file urls.txt \
  --dl-concurrent 30 \
  --workers 16 \
  --dpi 150 \
  --log-level DEBUG \
  --output-dir "/caminho/para/saida" \
  --combine
```

## Parâmetros de Configuração

### Parâmetros Globais

- `--log-level`: Nível de logging (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- `--log-file`: Arquivo de log (padrão: logs/image_processor.log)
- `--output-dir`, `-o`: Diretório base para saída (padrão: output)

### Parâmetros do Downloader

- `--dl-timeout`: Tempo limite para requisições em segundos (padrão: 30)
- `--dl-concurrent`: Número máximo de downloads concorrentes (padrão: 20)

### Parâmetros do Conversor

- `--dpi`: Resolução DPI para a conversão (padrão: 300)
- `--workers`: Número máximo de workers para processamento (padrão: 8)

### Parâmetros Específicos do Comando `file`

- `--batch-size`, `-b`: Tamanho do lote para processamento (padrão: 100)
- `--combine`, `-c`: Combinar todas as imagens em um único PDF

## Otimização de Desempenho

Para obter o melhor desempenho durante o processamento de grandes volumes de imagens:

1. **Ajuste o número de downloads concorrentes**:
   - Aumente para conexões de internet rápidas e estáveis (ex: 30-50)
   - Diminua para conexões mais lentas (ex: 10-15)

2. **Ajuste o número de workers**:
   - Idealmente, defina um valor próximo ao número de núcleos de CPU
   - Para sistemas com 4 núcleos: 4-8 workers
   - Para sistemas com 8+ núcleos: 8-16 workers

3. **Ajuste o tamanho do lote**:
   - Lotes menores (50-100) para sistemas com menos memória
   - Lotes maiores (200-500) para sistemas com mais memória e melhor monitoramento de progresso

4. **Ajuste a resolução DPI**:
   - Valores mais altos (300-600) para melhor qualidade
   - Valores mais baixos (150-200) para arquivos menores e processamento mais rápido

## Exemplos de Arquivo de URLs

Crie um arquivo de texto contendo uma URL por linha:

```
https://exemplo.com/imagem1.jpg
https://exemplo.com/imagem2.png
https://exemplo.com/imagem3.webp
...
```

## Tratamento de Erros

A aplicação é projetada para ser robusta e lidar com falhas comuns:

- URLs inválidas
- Timeouts de conexão
- Formatos de arquivo não suportados
- Falhas durante a conversão

Mesmo se algumas imagens falharem, o processamento continuará para o restante.

## Como Funciona

1. **Download de Imagens**:
   - Utiliza `aiohttp` para realizar downloads assíncronos
   - Implementa semáforos para limitar a concorrência
   - Detecta automaticamente o tipo de conteúdo e extensão do arquivo

2. **Conversão para PDF**:
   - Utiliza a biblioteca Pillow (PIL) para manipulação de imagens
   - Converte para RGB se necessário
   - Processa as imagens em lotes paralelos usando ThreadPoolExecutor

3. **Monitoramento e Estatísticas**:
   - Tracking de tempo total e tempo médio por arquivo
   - Estatísticas de taxa de sucesso
   - Feedback de progresso em tempo real

## Limitações

- Apenas formatos de imagem suportados pela biblioteca Pillow podem ser convertidos (PNG, JPEG, TIFF, BMP, WebP)
- A resolução máxima das imagens é limitada pela memória disponível
