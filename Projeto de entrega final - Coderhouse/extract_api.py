import pandas as pd
import requests
import sqlite3 as sql
from datetime import datetime
from plyer import notification

# Função de alerta
def alert(level, base, step):
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

# Função para extrair os dados da API
def extract_api(url):
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
    df = pd.DataFrame(output)
    df.to_csv(f'{csv_name}.csv', index=False)
    print(f'Arquivo {csv_name}.csv criado.')
    return df

# Carregamento dos dados no banco de dados
def upload_to_db(df, db_name='../data_f1.db', table_name='nome_tabela'):
    try:
        conn = sql.connect(db_name)
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        conn.close()
        print(f"Database {db_name} alimentada! Atualizada com a tabela {table_name}")
    except Exception as e:
        print(f'Erro ao salvar dados no banco de dados: {e}')

# Função que chama todas as outras
def process_api_data(url, csv_name, table_name, db_name='../data_f1.db'):
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

