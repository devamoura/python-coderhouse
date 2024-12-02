
# Projeto de Extração de Dados de APIs e Carregamento em Banco de Dados

## Descrição
Este projeto em Python realiza a extração de dados de APIs, salva os dados em arquivos CSV e armazena-os em um banco de dados SQLite. Além disso, é possível realizar consultas no banco para visualizar os dados salvos.

## Dependências
- `pandas`
- `requests`
- `sqlite3`
- `datetime`
- `plyer`

## Instalação
```bash
pip install pandas requests plyer
```

## Como Executar
1. **Extração e Carregamento**:
   - Os dados das APIs são extraídos, salvos como CSV e inseridos no banco de dados SQLite executando:
     ```python
     python extract_api.py
     ```

2. **Consultas no Banco de Dados**:
   - Você pode visualizar os dados executando a função `load_all_tables`.

## APIs Utilizadas
- `https://api.openf1.org/v1/meetings`
- `https://api.openf1.org/v1/drivers`
- `https://api.openf1.org/v1/sessions`
