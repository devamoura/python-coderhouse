import pandas as pd
import requests
import sqlite3 as sql
from datetime import datetime
from plyer import notification

# Função de alerta
def alert(level, base, step):
    """
    Envia uma notificação de alerta para o sistema.

    Args:
        level (int): Nível do alerta (1 - baixo, 2 - médio, 3 - alto).
        base (str): Nome da base de dados ou API sendo processada.
        step (str): Etapa em que o erro ocorreu (e.g., 'Extração').

    Returns:
        None
    """
    now = str(datetime.now())
    msg = f'Falha no carregamento da base {base} na etapa {step}.'

    if level == 1:
        title = 'ERRO! Alerta Baixo'
    elif level == 2:
        title = 'ERRO! Alerta Médio'
    elif level == 3:
        title = 'ERRO! Alerta Alto'
    else:
        print(f'Nível {level} não disponível!')

    notification.notify(
        title=title,
        message=msg,
        app_name='extract_api',
        timeout=15
    )

# Função para tratamento de dados extraídos da API
def clean_data(data):
    """
    Realiza o tratamento de dados extraídos da API.

    Args:
        data (dict): Dados brutos retornados pela API.

    Returns:
        pd.DataFrame: DataFrame tratado.
    """
    try:
        df = pd.DataFrame(data)
        
        columns_to_drop = [col for col in df.columns if 'unnecessary' in col.lower()]
        df.drop(columns=columns_to_drop, inplace=True, errors='ignore')

        
        df.fillna('Desconhecido', inplace=True)

        
        df.rename(columns=lambda x: x.strip().lower().replace(' ', '_'), inplace=True)

        
        df.drop_duplicates(inplace=True)
        
        return df
    except Exception as e:
        alert(3, 'Tratamento de Dados', 'Limpeza')
        print(f'Erro ao tratar os dados: {e}')
        return pd.DataFrame()

# Função para extrair os dados da API
def extract_api(url):
    """
    Extrai dados de uma API fornecida.

    Args:
        url (str): URL da API a ser acessada.

    Returns:
        dict or None: Dados da API em formato JSON se bem-sucedido, caso contrário, None.
    """
    try:
        resp = requests.get(url)
        if resp.status_code == 200:
            return resp.json()
        else:
            alert(3, url, 'Extração')
            print(f'Falha ao buscar dados da API. Código do status: {resp.status_code}')
            return None
    except Exception as e:
        alert(3, url, 'Extração')
        print(f'Erro ao acessar a API: {e}')
        return None

# Criação do dataframe a partir dos dados
def dataframe_create(output, csv_name):

    df = clean_data(output)
    df.to_csv(f'{csv_name}.csv', index=False)
    print(f'Arquivo {csv_name}.csv criado.')
    return df

# Carregamento dos dados no banco de dados
def upload_to_db(df, db_name='../data_f1.db', table_name='nome_tabela'):
    """
    Carrega um DataFrame para um banco de dados SQLite.

    Args:
        df (pd.DataFrame): DataFrame a ser carregado no banco de dados.
        db_name (str): Caminho para o banco de dados SQLite.
        table_name (str): Nome da tabela a ser criada/atualizada.

    Returns:
        None
    """
    try:
        conn = sql.connect(db_name)
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        conn.close()
        print(f"Database {db_name} alimentada! Atualizada com a tabela {table_name}")
    except Exception as e:
        print(f'Erro ao salvar dados no banco de dados: {e}')

# Função que chama todas as outras
def process_api_data(url, csv_name, table_name, db_name='../data_f1.db'):
    """
    Processa os dados da API, salvando-os como CSV e carregando-os em um banco de dados.

    Args:
        url (str): URL da API a ser processada.
        csv_name (str): Nome do arquivo CSV a ser criado.
        table_name (str): Nome da tabela no banco de dados.
        db_name (str): Caminho para o banco de dados SQLite.

    Returns:
        None
    """
    output = extract_api(url)
    if output:
        df = dataframe_create(output, csv_name)
        upload_to_db(df, db_name, table_name)

# APIs
url_meetings_f1 = 'https://api.openf1.org/v1/meetings'
url_drivers_f1 = 'https://api.openf1.org/v1/drivers'
url_sessions_f1 = 'https://api.openf1.org/v1/sessions'

# Processar os dados e salvar no banco
process_api_data(url_meetings_f1, 'meetings_f1', 'meetings_f1')
process_api_data(url_drivers_f1, 'drivers_f1', 'drivers_f1')
process_api_data(url_sessions_f1, 'sessions_f1', 'sessions_f1')

# Função de conexão no banco, query e logoff
def load_from_db(query, db_name='../data_f1.db'):
    """
    Carrega dados de uma tabela no banco de dados com base em uma query.

    Args:
        query (str): Consulta SQL para buscar os dados.
        db_name (str): Caminho para o banco de dados SQLite.

    Returns:
        pd.DataFrame or None: DataFrame com os resultados da consulta, ou None em caso de erro.
    """
    try:
        conn = sql.connect(db_name)
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except Exception as e:
        print(f'Erro ao carregar dados do banco: {e}')
        return None

# Função que executa as querys 
def load_all_tables(tables, db_name='../data_f1.db'):
    """
    Carrega e exibe dados de múltiplas tabelas no banco de dados.

    Args:
        tables (list): Lista de nomes das tabelas a serem carregadas.
        db_name (str): Caminho para o banco de dados SQLite.

    Returns:
        None
    """
    for table in tables:
        print(f"\nDados da tabela: {table}")
        query = f"SELECT * FROM {table}"
        df = load_from_db(query, db_name)
        if df is not None:
            print(df)
        else:
            print(f"Erro ao carregar dados da tabela {table} ou tabela não encontrada.")


tables = ['meetings_f1', 'drivers_f1', 'sessions_f1']

load_all_tables(tables)