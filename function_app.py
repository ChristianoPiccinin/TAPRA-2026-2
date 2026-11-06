import csv
import io
import logging
import os
from datetime import datetime, timezone

import pyodbc
import azure.functions as func
from azure.storage.filedatalake import DataLakeServiceClient

app = func.FunctionApp()


# ======================================================================
# itsm.categoria
# ======================================================================
@app.timer_trigger(schedule="0 * * * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False)
def extract_categoria(myTimer: func.TimerRequest) -> None:
    logging.info('tabela categoria')

    # ------------------------------------------------------------------
    # 1. Variáveis de ambiente (configuradas no Function App)
    # ------------------------------------------------------------------
    # Banco de origem
    sql_server = os.getenv("HOST")
    sql_database = os.getenv("DATABASE")
    sql_user = os.getenv("USER")
    sql_pass = os.getenv("PASSWORD")

    # Banco de destino
    dest_server = os.getenv("DEST_HOST")
    dest_database = os.getenv("DEST_DATABASE")
    dest_user = os.getenv("DEST_USER")
    dest_pass = os.getenv("DEST_PASSWORD")

    # ------------------------------------------------------------------
    # 2. Strings de conexão
    # ------------------------------------------------------------------
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={sql_server};"
        f"DATABASE={sql_database};"
        f"UID={sql_user};"
        f"PWD={sql_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    dest_conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={dest_server};"
        f"DATABASE={dest_database};"
        f"UID={dest_user};"
        f"PWD={dest_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    try:
        # ------------------------------------------------------------------
        # 3. Leitura da tabela no banco de origem
        # ------------------------------------------------------------------
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()

            query = "select * from itsm.categoria"
            cursor.execute(query)

            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            logging.info(f'{len(rows)} linhas lidas de itsm.categoria')

        # ------------------------------------------------------------------
        # 4. Gravação do CSV no contêiner bronze do Data Lake
        # ------------------------------------------------------------------
        # Cria um buffer de memória e escreve cabeçalho + linhas em CSV
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(columns)
        writer.writerows(rows)

        # Caminho particionado por data
        now = datetime.now(timezone.utc)
        file_path = f"categoria/{now:%Y/%m/%d}/categoria_{now:%Y%m%d_%H%M%S}.csv"

        #itsm/categoria/year=2024/month=06/day=05/categoria_20240605_123456.csv
        #file_path = f"itsm/categoria/year={now:%Y}/month={now:%m}/day={now:%d}/categoria_{now:%Y%m%d_%H%M%S}.csv"

        datalake = DataLakeServiceClient.from_connection_string(os.getenv("DATALAKE_CONNECTION_STRING"))
        file_client = datalake.get_file_system_client("bronze").get_file_client(file_path)
        file_client.upload_data(buffer.getvalue().encode("utf-8"), overwrite=True)

        logging.info(f'Arquivo gravado em bronze/{file_path}')

        # ------------------------------------------------------------------
        # 5. Gravação no banco de dados de destino (full refresh)
        # ------------------------------------------------------------------
        # As tabelas de destino não têm constraints; para não duplicar dados a
        # cada execução, faz DELETE + INSERT na mesma transação (rollback em erro).
        col_list = ", ".join(f"[{c}]" for c in columns)
        placeholders = ", ".join("?" for _ in columns)
        insert_sql = f"INSERT INTO itsm.categoria ({col_list}) VALUES ({placeholders})"

        dest_conn = pyodbc.connect(dest_conn_str, autocommit=False)
        try:
            dest_cursor = dest_conn.cursor()

            # Verifica se a tabela tem coluna IDENTITY (para manter os ids da origem)
            dest_cursor.execute("SELECT OBJECTPROPERTY(OBJECT_ID('itsm.categoria'), 'TableHasIdentity')")
            has_identity = dest_cursor.fetchone()[0] == 1

            dest_cursor.execute("DELETE FROM itsm.categoria")

            if has_identity:
                dest_cursor.execute("SET IDENTITY_INSERT itsm.categoria ON")

            if rows:
                dest_cursor.fast_executemany = True
                dest_cursor.executemany(insert_sql, [tuple(r) for r in rows])

            if has_identity:
                dest_cursor.execute("SET IDENTITY_INSERT itsm.categoria OFF")

            dest_conn.commit()
            logging.info(f'{len(rows)} linhas gravadas em itsm.categoria no banco de destino')
        except Exception:
            dest_conn.rollback()
            raise
        finally:
            dest_conn.close()

    except Exception as e:
        logging.error(f"Erro ao processar itsm.categoria: {str(e)}")
        raise


# ======================================================================
# itsm.cliente_organizacao
# ======================================================================
@app.timer_trigger(schedule="0 * * * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False)
def extract_cliente_organizacao(myTimer: func.TimerRequest) -> None:
    logging.info('tabela cliente_organizacao')

    # ------------------------------------------------------------------
    # 1. Variáveis de ambiente (configuradas no Function App)
    # ------------------------------------------------------------------
    # Banco de origem
    sql_server = os.getenv("HOST")
    sql_database = os.getenv("DATABASE")
    sql_user = os.getenv("USER")
    sql_pass = os.getenv("PASSWORD")

    # Banco de destino
    dest_server = os.getenv("DEST_HOST")
    dest_database = os.getenv("DEST_DATABASE")
    dest_user = os.getenv("DEST_USER")
    dest_pass = os.getenv("DEST_PASSWORD")

    # ------------------------------------------------------------------
    # 2. Strings de conexão
    # ------------------------------------------------------------------
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={sql_server};"
        f"DATABASE={sql_database};"
        f"UID={sql_user};"
        f"PWD={sql_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    dest_conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={dest_server};"
        f"DATABASE={dest_database};"
        f"UID={dest_user};"
        f"PWD={dest_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    try:
        # ------------------------------------------------------------------
        # 3. Leitura da tabela no banco de origem
        # ------------------------------------------------------------------
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()

            query = "select * from itsm.cliente_organizacao"
            cursor.execute(query)

            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            logging.info(f'{len(rows)} linhas lidas de itsm.cliente_organizacao')

        # ------------------------------------------------------------------
        # 4. Gravação do CSV no contêiner bronze do Data Lake
        # ------------------------------------------------------------------
        # Cria um buffer de memória e escreve cabeçalho + linhas em CSV
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(columns)
        writer.writerows(rows)

        # Caminho particionado por data
        now = datetime.now(timezone.utc)
        file_path = f"cliente_organizacao/{now:%Y/%m/%d}/cliente_organizacao_{now:%Y%m%d_%H%M%S}.csv"

        #itsm/cliente_organizacao/year=2024/month=06/day=05/cliente_organizacao_20240605_123456.csv
        #file_path = f"itsm/cliente_organizacao/year={now:%Y}/month={now:%m}/day={now:%d}/cliente_organizacao_{now:%Y%m%d_%H%M%S}.csv"

        datalake = DataLakeServiceClient.from_connection_string(os.getenv("DATALAKE_CONNECTION_STRING"))
        file_client = datalake.get_file_system_client("bronze").get_file_client(file_path)
        file_client.upload_data(buffer.getvalue().encode("utf-8"), overwrite=True)

        logging.info(f'Arquivo gravado em bronze/{file_path}')

        # ------------------------------------------------------------------
        # 5. Gravação no banco de dados de destino (full refresh)
        # ------------------------------------------------------------------
        # As tabelas de destino não têm constraints; para não duplicar dados a
        # cada execução, faz DELETE + INSERT na mesma transação (rollback em erro).
        col_list = ", ".join(f"[{c}]" for c in columns)
        placeholders = ", ".join("?" for _ in columns)
        insert_sql = f"INSERT INTO itsm.cliente_organizacao ({col_list}) VALUES ({placeholders})"

        dest_conn = pyodbc.connect(dest_conn_str, autocommit=False)
        try:
            dest_cursor = dest_conn.cursor()

            # Verifica se a tabela tem coluna IDENTITY (para manter os ids da origem)
            dest_cursor.execute("SELECT OBJECTPROPERTY(OBJECT_ID('itsm.cliente_organizacao'), 'TableHasIdentity')")
            has_identity = dest_cursor.fetchone()[0] == 1

            dest_cursor.execute("DELETE FROM itsm.cliente_organizacao")

            if has_identity:
                dest_cursor.execute("SET IDENTITY_INSERT itsm.cliente_organizacao ON")

            if rows:
                dest_cursor.fast_executemany = True
                dest_cursor.executemany(insert_sql, [tuple(r) for r in rows])

            if has_identity:
                dest_cursor.execute("SET IDENTITY_INSERT itsm.cliente_organizacao OFF")

            dest_conn.commit()
            logging.info(f'{len(rows)} linhas gravadas em itsm.cliente_organizacao no banco de destino')
        except Exception:
            dest_conn.rollback()
            raise
        finally:
            dest_conn.close()

    except Exception as e:
        logging.error(f"Erro ao processar itsm.cliente_organizacao: {str(e)}")
        raise


# ======================================================================
# itsm.csat
# ======================================================================
@app.timer_trigger(schedule="0 * * * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False)
def extract_csat(myTimer: func.TimerRequest) -> None:
    logging.info('tabela csat')

    # ------------------------------------------------------------------
    # 1. Variáveis de ambiente (configuradas no Function App)
    # ------------------------------------------------------------------
    # Banco de origem
    sql_server = os.getenv("HOST")
    sql_database = os.getenv("DATABASE")
    sql_user = os.getenv("USER")
    sql_pass = os.getenv("PASSWORD")

    # Banco de destino
    dest_server = os.getenv("DEST_HOST")
    dest_database = os.getenv("DEST_DATABASE")
    dest_user = os.getenv("DEST_USER")
    dest_pass = os.getenv("DEST_PASSWORD")

    # ------------------------------------------------------------------
    # 2. Strings de conexão
    # ------------------------------------------------------------------
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={sql_server};"
        f"DATABASE={sql_database};"
        f"UID={sql_user};"
        f"PWD={sql_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    dest_conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={dest_server};"
        f"DATABASE={dest_database};"
        f"UID={dest_user};"
        f"PWD={dest_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    try:
        # ------------------------------------------------------------------
        # 3. Leitura da tabela no banco de origem
        # ------------------------------------------------------------------
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()

            query = "select * from itsm.csat"
            cursor.execute(query)

            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            logging.info(f'{len(rows)} linhas lidas de itsm.csat')

        # ------------------------------------------------------------------
        # 4. Gravação do CSV no contêiner bronze do Data Lake
        # ------------------------------------------------------------------
        # Cria um buffer de memória e escreve cabeçalho + linhas em CSV
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(columns)
        writer.writerows(rows)

        # Caminho particionado por data
        now = datetime.now(timezone.utc)
        file_path = f"csat/{now:%Y/%m/%d}/csat_{now:%Y%m%d_%H%M%S}.csv"

        #itsm/csat/year=2024/month=06/day=05/csat_20240605_123456.csv
        #file_path = f"itsm/csat/year={now:%Y}/month={now:%m}/day={now:%d}/csat_{now:%Y%m%d_%H%M%S}.csv"

        datalake = DataLakeServiceClient.from_connection_string(os.getenv("DATALAKE_CONNECTION_STRING"))
        file_client = datalake.get_file_system_client("bronze").get_file_client(file_path)
        file_client.upload_data(buffer.getvalue().encode("utf-8"), overwrite=True)

        logging.info(f'Arquivo gravado em bronze/{file_path}')

        # ------------------------------------------------------------------
        # 5. Gravação no banco de dados de destino (full refresh)
        # ------------------------------------------------------------------
        # As tabelas de destino não têm constraints; para não duplicar dados a
        # cada execução, faz DELETE + INSERT na mesma transação (rollback em erro).
        col_list = ", ".join(f"[{c}]" for c in columns)
        placeholders = ", ".join("?" for _ in columns)
        insert_sql = f"INSERT INTO itsm.csat ({col_list}) VALUES ({placeholders})"

        dest_conn = pyodbc.connect(dest_conn_str, autocommit=False)
        try:
            dest_cursor = dest_conn.cursor()

            # Verifica se a tabela tem coluna IDENTITY (para manter os ids da origem)
            dest_cursor.execute("SELECT OBJECTPROPERTY(OBJECT_ID('itsm.csat'), 'TableHasIdentity')")
            has_identity = dest_cursor.fetchone()[0] == 1

            dest_cursor.execute("DELETE FROM itsm.csat")

            if has_identity:
                dest_cursor.execute("SET IDENTITY_INSERT itsm.csat ON")

            if rows:
                dest_cursor.fast_executemany = True
                dest_cursor.executemany(insert_sql, [tuple(r) for r in rows])

            if has_identity:
                dest_cursor.execute("SET IDENTITY_INSERT itsm.csat OFF")

            dest_conn.commit()
            logging.info(f'{len(rows)} linhas gravadas em itsm.csat no banco de destino')
        except Exception:
            dest_conn.rollback()
            raise
        finally:
            dest_conn.close()

    except Exception as e:
        logging.error(f"Erro ao processar itsm.csat: {str(e)}")
        raise


# ======================================================================
# itsm.fila
# ======================================================================
@app.timer_trigger(schedule="0 * * * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False)
def extract_fila(myTimer: func.TimerRequest) -> None:
    logging.info('tabela fila')

    # ------------------------------------------------------------------
    # 1. Variáveis de ambiente (configuradas no Function App)
    # ------------------------------------------------------------------
    # Banco de origem
    sql_server = os.getenv("HOST")
    sql_database = os.getenv("DATABASE")
    sql_user = os.getenv("USER")
    sql_pass = os.getenv("PASSWORD")

    # Banco de destino
    dest_server = os.getenv("DEST_HOST")
    dest_database = os.getenv("DEST_DATABASE")
    dest_user = os.getenv("DEST_USER")
    dest_pass = os.getenv("DEST_PASSWORD")

    # ------------------------------------------------------------------
    # 2. Strings de conexão
    # ------------------------------------------------------------------
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={sql_server};"
        f"DATABASE={sql_database};"
        f"UID={sql_user};"
        f"PWD={sql_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    dest_conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={dest_server};"
        f"DATABASE={dest_database};"
        f"UID={dest_user};"
        f"PWD={dest_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    try:
        # ------------------------------------------------------------------
        # 3. Leitura da tabela no banco de origem
        # ------------------------------------------------------------------
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()

            query = "select * from itsm.fila"
            cursor.execute(query)

            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            logging.info(f'{len(rows)} linhas lidas de itsm.fila')

        # ------------------------------------------------------------------
        # 4. Gravação do CSV no contêiner bronze do Data Lake
        # ------------------------------------------------------------------
        # Cria um buffer de memória e escreve cabeçalho + linhas em CSV
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(columns)
        writer.writerows(rows)

        # Caminho particionado por data
        now = datetime.now(timezone.utc)
        file_path = f"fila/{now:%Y/%m/%d}/fila_{now:%Y%m%d_%H%M%S}.csv"

        #itsm/fila/year=2024/month=06/day=05/fila_20240605_123456.csv
        #file_path = f"itsm/fila/year={now:%Y}/month={now:%m}/day={now:%d}/fila_{now:%Y%m%d_%H%M%S}.csv"

        datalake = DataLakeServiceClient.from_connection_string(os.getenv("DATALAKE_CONNECTION_STRING"))
        file_client = datalake.get_file_system_client("bronze").get_file_client(file_path)
        file_client.upload_data(buffer.getvalue().encode("utf-8"), overwrite=True)

        logging.info(f'Arquivo gravado em bronze/{file_path}')

        # ------------------------------------------------------------------
        # 5. Gravação no banco de dados de destino (full refresh)
        # ------------------------------------------------------------------
        # As tabelas de destino não têm constraints; para não duplicar dados a
        # cada execução, faz DELETE + INSERT na mesma transação (rollback em erro).
        col_list = ", ".join(f"[{c}]" for c in columns)
        placeholders = ", ".join("?" for _ in columns)
        insert_sql = f"INSERT INTO itsm.fila ({col_list}) VALUES ({placeholders})"

        dest_conn = pyodbc.connect(dest_conn_str, autocommit=False)
        try:
            dest_cursor = dest_conn.cursor()

            # Verifica se a tabela tem coluna IDENTITY (para manter os ids da origem)
            dest_cursor.execute("SELECT OBJECTPROPERTY(OBJECT_ID('itsm.fila'), 'TableHasIdentity')")
            has_identity = dest_cursor.fetchone()[0] == 1

            dest_cursor.execute("DELETE FROM itsm.fila")

            if has_identity:
                dest_cursor.execute("SET IDENTITY_INSERT itsm.fila ON")

            if rows:
                dest_cursor.fast_executemany = True
                dest_cursor.executemany(insert_sql, [tuple(r) for r in rows])

            if has_identity:
                dest_cursor.execute("SET IDENTITY_INSERT itsm.fila OFF")

            dest_conn.commit()
            logging.info(f'{len(rows)} linhas gravadas em itsm.fila no banco de destino')
        except Exception:
            dest_conn.rollback()
            raise
        finally:
            dest_conn.close()

    except Exception as e:
        logging.error(f"Erro ao processar itsm.fila: {str(e)}")
        raise


# ======================================================================
# itsm.sla
# ======================================================================
@app.timer_trigger(schedule="0 * * * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False)
def extract_sla(myTimer: func.TimerRequest) -> None:
    logging.info('tabela sla')

    # ------------------------------------------------------------------
    # 1. Variáveis de ambiente (configuradas no Function App)
    # ------------------------------------------------------------------
    # Banco de origem
    sql_server = os.getenv("HOST")
    sql_database = os.getenv("DATABASE")
    sql_user = os.getenv("USER")
    sql_pass = os.getenv("PASSWORD")

    # Banco de destino
    dest_server = os.getenv("DEST_HOST")
    dest_database = os.getenv("DEST_DATABASE")
    dest_user = os.getenv("DEST_USER")
    dest_pass = os.getenv("DEST_PASSWORD")

    # ------------------------------------------------------------------
    # 2. Strings de conexão
    # ------------------------------------------------------------------
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={sql_server};"
        f"DATABASE={sql_database};"
        f"UID={sql_user};"
        f"PWD={sql_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    dest_conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={dest_server};"
        f"DATABASE={dest_database};"
        f"UID={dest_user};"
        f"PWD={dest_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    try:
        # ------------------------------------------------------------------
        # 3. Leitura da tabela no banco de origem
        # ------------------------------------------------------------------
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()

            query = "select * from itsm.sla"
            cursor.execute(query)

            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            logging.info(f'{len(rows)} linhas lidas de itsm.sla')

        # ------------------------------------------------------------------
        # 4. Gravação do CSV no contêiner bronze do Data Lake
        # ------------------------------------------------------------------
        # Cria um buffer de memória e escreve cabeçalho + linhas em CSV
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(columns)
        writer.writerows(rows)

        # Caminho particionado por data
        now = datetime.now(timezone.utc)
        file_path = f"sla/{now:%Y/%m/%d}/sla_{now:%Y%m%d_%H%M%S}.csv"

        #itsm/sla/year=2024/month=06/day=05/sla_20240605_123456.csv
        #file_path = f"itsm/sla/year={now:%Y}/month={now:%m}/day={now:%d}/sla_{now:%Y%m%d_%H%M%S}.csv"

        datalake = DataLakeServiceClient.from_connection_string(os.getenv("DATALAKE_CONNECTION_STRING"))
        file_client = datalake.get_file_system_client("bronze").get_file_client(file_path)
        file_client.upload_data(buffer.getvalue().encode("utf-8"), overwrite=True)

        logging.info(f'Arquivo gravado em bronze/{file_path}')

        # ------------------------------------------------------------------
        # 5. Gravação no banco de dados de destino (full refresh)
        # ------------------------------------------------------------------
        # As tabelas de destino não têm constraints; para não duplicar dados a
        # cada execução, faz DELETE + INSERT na mesma transação (rollback em erro).
        col_list = ", ".join(f"[{c}]" for c in columns)
        placeholders = ", ".join("?" for _ in columns)
        insert_sql = f"INSERT INTO itsm.sla ({col_list}) VALUES ({placeholders})"

        dest_conn = pyodbc.connect(dest_conn_str, autocommit=False)
        try:
            dest_cursor = dest_conn.cursor()

            # Verifica se a tabela tem coluna IDENTITY (para manter os ids da origem)
            dest_cursor.execute("SELECT OBJECTPROPERTY(OBJECT_ID('itsm.sla'), 'TableHasIdentity')")
            has_identity = dest_cursor.fetchone()[0] == 1

            dest_cursor.execute("DELETE FROM itsm.sla")

            if has_identity:
                dest_cursor.execute("SET IDENTITY_INSERT itsm.sla ON")

            if rows:
                dest_cursor.fast_executemany = True
                dest_cursor.executemany(insert_sql, [tuple(r) for r in rows])

            if has_identity:
                dest_cursor.execute("SET IDENTITY_INSERT itsm.sla OFF")

            dest_conn.commit()
            logging.info(f'{len(rows)} linhas gravadas em itsm.sla no banco de destino')
        except Exception:
            dest_conn.rollback()
            raise
        finally:
            dest_conn.close()

    except Exception as e:
        logging.error(f"Erro ao processar itsm.sla: {str(e)}")
        raise


# ======================================================================
# itsm.analista
# ======================================================================
@app.timer_trigger(schedule="0 * * * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False)
def extract_analista(myTimer: func.TimerRequest) -> None:
    logging.info('tabela analista')

    # ------------------------------------------------------------------
    # 1. Variáveis de ambiente (configuradas no Function App)
    # ------------------------------------------------------------------
    # Banco de origem
    sql_server = os.getenv("HOST")
    sql_database = os.getenv("DATABASE")
    sql_user = os.getenv("USER")
    sql_pass = os.getenv("PASSWORD")

    # Banco de destino
    dest_server = os.getenv("DEST_HOST")
    dest_database = os.getenv("DEST_DATABASE")
    dest_user = os.getenv("DEST_USER")
    dest_pass = os.getenv("DEST_PASSWORD")

    # ------------------------------------------------------------------
    # 2. Strings de conexão
    # ------------------------------------------------------------------
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={sql_server};"
        f"DATABASE={sql_database};"
        f"UID={sql_user};"
        f"PWD={sql_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    dest_conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={dest_server};"
        f"DATABASE={dest_database};"
        f"UID={dest_user};"
        f"PWD={dest_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    try:
        # ------------------------------------------------------------------
        # 3. Leitura da tabela no banco de origem
        # ------------------------------------------------------------------
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()

            query = "select * from itsm.analista"
            cursor.execute(query)

            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            logging.info(f'{len(rows)} linhas lidas de itsm.analista')

        # ------------------------------------------------------------------
        # 4. Gravação do CSV no contêiner bronze do Data Lake
        # ------------------------------------------------------------------
        # Cria um buffer de memória e escreve cabeçalho + linhas em CSV
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(columns)
        writer.writerows(rows)

        # Caminho particionado por data
        now = datetime.now(timezone.utc)
        file_path = f"analista/{now:%Y/%m/%d}/analista_{now:%Y%m%d_%H%M%S}.csv"

        #itsm/analista/year=2024/month=06/day=05/analista_20240605_123456.csv
        #file_path = f"itsm/analista/year={now:%Y}/month={now:%m}/day={now:%d}/analista_{now:%Y%m%d_%H%M%S}.csv"

        datalake = DataLakeServiceClient.from_connection_string(os.getenv("DATALAKE_CONNECTION_STRING"))
        file_client = datalake.get_file_system_client("bronze").get_file_client(file_path)
        file_client.upload_data(buffer.getvalue().encode("utf-8"), overwrite=True)

        logging.info(f'Arquivo gravado em bronze/{file_path}')

        # ------------------------------------------------------------------
        # 5. Gravação no banco de dados de destino (full refresh)
        # ------------------------------------------------------------------
        # As tabelas de destino não têm constraints; para não duplicar dados a
        # cada execução, faz DELETE + INSERT na mesma transação (rollback em erro).
        col_list = ", ".join(f"[{c}]" for c in columns)
        placeholders = ", ".join("?" for _ in columns)
        insert_sql = f"INSERT INTO itsm.analista ({col_list}) VALUES ({placeholders})"

        dest_conn = pyodbc.connect(dest_conn_str, autocommit=False)
        try:
            dest_cursor = dest_conn.cursor()

            # Verifica se a tabela tem coluna IDENTITY (para manter os ids da origem)
            dest_cursor.execute("SELECT OBJECTPROPERTY(OBJECT_ID('itsm.analista'), 'TableHasIdentity')")
            has_identity = dest_cursor.fetchone()[0] == 1

            dest_cursor.execute("DELETE FROM itsm.analista")

            if has_identity:
                dest_cursor.execute("SET IDENTITY_INSERT itsm.analista ON")

            if rows:
                dest_cursor.fast_executemany = True
                dest_cursor.executemany(insert_sql, [tuple(r) for r in rows])

            if has_identity:
                dest_cursor.execute("SET IDENTITY_INSERT itsm.analista OFF")

            dest_conn.commit()
            logging.info(f'{len(rows)} linhas gravadas em itsm.analista no banco de destino')
        except Exception:
            dest_conn.rollback()
            raise
        finally:
            dest_conn.close()

    except Exception as e:
        logging.error(f"Erro ao processar itsm.analista: {str(e)}")
        raise


# ======================================================================
# itsm.solicitante
# ======================================================================
@app.timer_trigger(schedule="0 * * * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False)
def extract_solicitante(myTimer: func.TimerRequest) -> None:
    logging.info('tabela solicitante')

    # ------------------------------------------------------------------
    # 1. Variáveis de ambiente (configuradas no Function App)
    # ------------------------------------------------------------------
    # Banco de origem
    sql_server = os.getenv("HOST")
    sql_database = os.getenv("DATABASE")
    sql_user = os.getenv("USER")
    sql_pass = os.getenv("PASSWORD")

    # Banco de destino
    dest_server = os.getenv("DEST_HOST")
    dest_database = os.getenv("DEST_DATABASE")
    dest_user = os.getenv("DEST_USER")
    dest_pass = os.getenv("DEST_PASSWORD")

    # ------------------------------------------------------------------
    # 2. Strings de conexão
    # ------------------------------------------------------------------
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={sql_server};"
        f"DATABASE={sql_database};"
        f"UID={sql_user};"
        f"PWD={sql_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    dest_conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={dest_server};"
        f"DATABASE={dest_database};"
        f"UID={dest_user};"
        f"PWD={dest_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    try:
        # ------------------------------------------------------------------
        # 3. Leitura da tabela no banco de origem
        # ------------------------------------------------------------------
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()

            query = "select * from itsm.solicitante"
            cursor.execute(query)

            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            logging.info(f'{len(rows)} linhas lidas de itsm.solicitante')

        # ------------------------------------------------------------------
        # 4. Gravação do CSV no contêiner bronze do Data Lake
        # ------------------------------------------------------------------
        # Cria um buffer de memória e escreve cabeçalho + linhas em CSV
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(columns)
        writer.writerows(rows)

        # Caminho particionado por data
        now = datetime.now(timezone.utc)
        file_path = f"solicitante/{now:%Y/%m/%d}/solicitante_{now:%Y%m%d_%H%M%S}.csv"

        #itsm/solicitante/year=2024/month=06/day=05/solicitante_20240605_123456.csv
        #file_path = f"itsm/solicitante/year={now:%Y}/month={now:%m}/day={now:%d}/solicitante_{now:%Y%m%d_%H%M%S}.csv"

        datalake = DataLakeServiceClient.from_connection_string(os.getenv("DATALAKE_CONNECTION_STRING"))
        file_client = datalake.get_file_system_client("bronze").get_file_client(file_path)
        file_client.upload_data(buffer.getvalue().encode("utf-8"), overwrite=True)

        logging.info(f'Arquivo gravado em bronze/{file_path}')

        # ------------------------------------------------------------------
        # 5. Gravação no banco de dados de destino (full refresh)
        # ------------------------------------------------------------------
        # As tabelas de destino não têm constraints; para não duplicar dados a
        # cada execução, faz DELETE + INSERT na mesma transação (rollback em erro).
        col_list = ", ".join(f"[{c}]" for c in columns)
        placeholders = ", ".join("?" for _ in columns)
        insert_sql = f"INSERT INTO itsm.solicitante ({col_list}) VALUES ({placeholders})"

        dest_conn = pyodbc.connect(dest_conn_str, autocommit=False)
        try:
            dest_cursor = dest_conn.cursor()

            # Verifica se a tabela tem coluna IDENTITY (para manter os ids da origem)
            dest_cursor.execute("SELECT OBJECTPROPERTY(OBJECT_ID('itsm.solicitante'), 'TableHasIdentity')")
            has_identity = dest_cursor.fetchone()[0] == 1

            dest_cursor.execute("DELETE FROM itsm.solicitante")

            if has_identity:
                dest_cursor.execute("SET IDENTITY_INSERT itsm.solicitante ON")

            if rows:
                dest_cursor.fast_executemany = True
                dest_cursor.executemany(insert_sql, [tuple(r) for r in rows])

            if has_identity:
                dest_cursor.execute("SET IDENTITY_INSERT itsm.solicitante OFF")

            dest_conn.commit()
            logging.info(f'{len(rows)} linhas gravadas em itsm.solicitante no banco de destino')
        except Exception:
            dest_conn.rollback()
            raise
        finally:
            dest_conn.close()

    except Exception as e:
        logging.error(f"Erro ao processar itsm.solicitante: {str(e)}")
        raise


# ======================================================================
# itsm.chamado
# ======================================================================
@app.timer_trigger(schedule="0 * * * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False)
def extract_chamado(myTimer: func.TimerRequest) -> None:
    logging.info('tabela chamado')

    # ------------------------------------------------------------------
    # 1. Variáveis de ambiente (configuradas no Function App)
    # ------------------------------------------------------------------
    # Banco de origem
    sql_server = os.getenv("HOST")
    sql_database = os.getenv("DATABASE")
    sql_user = os.getenv("USER")
    sql_pass = os.getenv("PASSWORD")

    # Banco de destino
    dest_server = os.getenv("DEST_HOST")
    dest_database = os.getenv("DEST_DATABASE")
    dest_user = os.getenv("DEST_USER")
    dest_pass = os.getenv("DEST_PASSWORD")

    # ------------------------------------------------------------------
    # 2. Strings de conexão
    # ------------------------------------------------------------------
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={sql_server};"
        f"DATABASE={sql_database};"
        f"UID={sql_user};"
        f"PWD={sql_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    dest_conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={dest_server};"
        f"DATABASE={dest_database};"
        f"UID={dest_user};"
        f"PWD={dest_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    try:
        # ------------------------------------------------------------------
        # 3. Leitura da tabela no banco de origem
        # ------------------------------------------------------------------
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()

            query = "select * from itsm.chamado"
            cursor.execute(query)

            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            logging.info(f'{len(rows)} linhas lidas de itsm.chamado')

        # ------------------------------------------------------------------
        # 4. Gravação do CSV no contêiner bronze do Data Lake
        # ------------------------------------------------------------------
        # Cria um buffer de memória e escreve cabeçalho + linhas em CSV
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(columns)
        writer.writerows(rows)

        # Caminho particionado por data
        now = datetime.now(timezone.utc)
        file_path = f"chamado/{now:%Y/%m/%d}/chamado_{now:%Y%m%d_%H%M%S}.csv"

        #itsm/chamado/year=2024/month=06/day=05/chamado_20240605_123456.csv
        #file_path = f"itsm/chamado/year={now:%Y}/month={now:%m}/day={now:%d}/chamado_{now:%Y%m%d_%H%M%S}.csv"

        datalake = DataLakeServiceClient.from_connection_string(os.getenv("DATALAKE_CONNECTION_STRING"))
        file_client = datalake.get_file_system_client("bronze").get_file_client(file_path)
        file_client.upload_data(buffer.getvalue().encode("utf-8"), overwrite=True)

        logging.info(f'Arquivo gravado em bronze/{file_path}')

        # ------------------------------------------------------------------
        # 5. Gravação no banco de dados de destino (full refresh)
        # ------------------------------------------------------------------
        # As tabelas de destino não têm constraints; para não duplicar dados a
        # cada execução, faz DELETE + INSERT na mesma transação (rollback em erro).
        col_list = ", ".join(f"[{c}]" for c in columns)
        placeholders = ", ".join("?" for _ in columns)
        insert_sql = f"INSERT INTO itsm.chamado ({col_list}) VALUES ({placeholders})"

        dest_conn = pyodbc.connect(dest_conn_str, autocommit=False)
        try:
            dest_cursor = dest_conn.cursor()

            # Verifica se a tabela tem coluna IDENTITY (para manter os ids da origem)
            dest_cursor.execute("SELECT OBJECTPROPERTY(OBJECT_ID('itsm.chamado'), 'TableHasIdentity')")
            has_identity = dest_cursor.fetchone()[0] == 1

            dest_cursor.execute("DELETE FROM itsm.chamado")

            if has_identity:
                dest_cursor.execute("SET IDENTITY_INSERT itsm.chamado ON")

            if rows:
                dest_cursor.fast_executemany = True
                dest_cursor.executemany(insert_sql, [tuple(r) for r in rows])

            if has_identity:
                dest_cursor.execute("SET IDENTITY_INSERT itsm.chamado OFF")

            dest_conn.commit()
            logging.info(f'{len(rows)} linhas gravadas em itsm.chamado no banco de destino')
        except Exception:
            dest_conn.rollback()
            raise
        finally:
            dest_conn.close()

    except Exception as e:
        logging.error(f"Erro ao processar itsm.chamado: {str(e)}")
        raise


# ======================================================================
# itsm.chamado_sla
# ======================================================================
@app.timer_trigger(schedule="0 * * * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False)
def extract_chamado_sla(myTimer: func.TimerRequest) -> None:
    logging.info('tabela chamado_sla')

    # ------------------------------------------------------------------
    # 1. Variáveis de ambiente (configuradas no Function App)
    # ------------------------------------------------------------------
    # Banco de origem
    sql_server = os.getenv("HOST")
    sql_database = os.getenv("DATABASE")
    sql_user = os.getenv("USER")
    sql_pass = os.getenv("PASSWORD")

    # Banco de destino
    dest_server = os.getenv("DEST_HOST")
    dest_database = os.getenv("DEST_DATABASE")
    dest_user = os.getenv("DEST_USER")
    dest_pass = os.getenv("DEST_PASSWORD")

    # ------------------------------------------------------------------
    # 2. Strings de conexão
    # ------------------------------------------------------------------
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={sql_server};"
        f"DATABASE={sql_database};"
        f"UID={sql_user};"
        f"PWD={sql_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    dest_conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={dest_server};"
        f"DATABASE={dest_database};"
        f"UID={dest_user};"
        f"PWD={dest_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    try:
        # ------------------------------------------------------------------
        # 3. Leitura da tabela no banco de origem
        # ------------------------------------------------------------------
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()

            query = "select * from itsm.chamado_sla"
            cursor.execute(query)

            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            logging.info(f'{len(rows)} linhas lidas de itsm.chamado_sla')

        # ------------------------------------------------------------------
        # 4. Gravação do CSV no contêiner bronze do Data Lake
        # ------------------------------------------------------------------
        # Cria um buffer de memória e escreve cabeçalho + linhas em CSV
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(columns)
        writer.writerows(rows)

        # Caminho particionado por data
        now = datetime.now(timezone.utc)
        file_path = f"chamado_sla/{now:%Y/%m/%d}/chamado_sla_{now:%Y%m%d_%H%M%S}.csv"

        #itsm/chamado_sla/year=2024/month=06/day=05/chamado_sla_20240605_123456.csv
        #file_path = f"itsm/chamado_sla/year={now:%Y}/month={now:%m}/day={now:%d}/chamado_sla_{now:%Y%m%d_%H%M%S}.csv"

        datalake = DataLakeServiceClient.from_connection_string(os.getenv("DATALAKE_CONNECTION_STRING"))
        file_client = datalake.get_file_system_client("bronze").get_file_client(file_path)
        file_client.upload_data(buffer.getvalue().encode("utf-8"), overwrite=True)

        logging.info(f'Arquivo gravado em bronze/{file_path}')

        # ------------------------------------------------------------------
        # 5. Gravação no banco de dados de destino (full refresh)
        # ------------------------------------------------------------------
        # As tabelas de destino não têm constraints; para não duplicar dados a
        # cada execução, faz DELETE + INSERT na mesma transação (rollback em erro).
        col_list = ", ".join(f"[{c}]" for c in columns)
        placeholders = ", ".join("?" for _ in columns)
        insert_sql = f"INSERT INTO itsm.chamado_sla ({col_list}) VALUES ({placeholders})"

        dest_conn = pyodbc.connect(dest_conn_str, autocommit=False)
        try:
            dest_cursor = dest_conn.cursor()

            # Verifica se a tabela tem coluna IDENTITY (para manter os ids da origem)
            dest_cursor.execute("SELECT OBJECTPROPERTY(OBJECT_ID('itsm.chamado_sla'), 'TableHasIdentity')")
            has_identity = dest_cursor.fetchone()[0] == 1

            dest_cursor.execute("DELETE FROM itsm.chamado_sla")

            if has_identity:
                dest_cursor.execute("SET IDENTITY_INSERT itsm.chamado_sla ON")

            if rows:
                dest_cursor.fast_executemany = True
                dest_cursor.executemany(insert_sql, [tuple(r) for r in rows])

            if has_identity:
                dest_cursor.execute("SET IDENTITY_INSERT itsm.chamado_sla OFF")

            dest_conn.commit()
            logging.info(f'{len(rows)} linhas gravadas em itsm.chamado_sla no banco de destino')
        except Exception:
            dest_conn.rollback()
            raise
        finally:
            dest_conn.close()

    except Exception as e:
        logging.error(f"Erro ao processar itsm.chamado_sla: {str(e)}")
        raise


# ======================================================================
# itsm.chamado_status_historico
# ======================================================================
@app.timer_trigger(schedule="0 * * * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False)
def extract_chamado_status_historico(myTimer: func.TimerRequest) -> None:
    logging.info('tabela chamado_status_historico')

    # ------------------------------------------------------------------
    # 1. Variáveis de ambiente (configuradas no Function App)
    # ------------------------------------------------------------------
    # Banco de origem
    sql_server = os.getenv("HOST")
    sql_database = os.getenv("DATABASE")
    sql_user = os.getenv("USER")
    sql_pass = os.getenv("PASSWORD")

    # Banco de destino
    dest_server = os.getenv("DEST_HOST")
    dest_database = os.getenv("DEST_DATABASE")
    dest_user = os.getenv("DEST_USER")
    dest_pass = os.getenv("DEST_PASSWORD")

    # ------------------------------------------------------------------
    # 2. Strings de conexão
    # ------------------------------------------------------------------
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={sql_server};"
        f"DATABASE={sql_database};"
        f"UID={sql_user};"
        f"PWD={sql_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    dest_conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={dest_server};"
        f"DATABASE={dest_database};"
        f"UID={dest_user};"
        f"PWD={dest_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    try:
        # ------------------------------------------------------------------
        # 3. Leitura da tabela no banco de origem
        # ------------------------------------------------------------------
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()

            query = "select * from itsm.chamado_status_historico"
            cursor.execute(query)

            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            logging.info(f'{len(rows)} linhas lidas de itsm.chamado_status_historico')

        # ------------------------------------------------------------------
        # 4. Gravação do CSV no contêiner bronze do Data Lake
        # ------------------------------------------------------------------
        # Cria um buffer de memória e escreve cabeçalho + linhas em CSV
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(columns)
        writer.writerows(rows)

        # Caminho particionado por data
        now = datetime.now(timezone.utc)
        file_path = f"chamado_status_historico/{now:%Y/%m/%d}/chamado_status_historico_{now:%Y%m%d_%H%M%S}.csv"

        #itsm/chamado_status_historico/year=2024/month=06/day=05/chamado_status_historico_20240605_123456.csv
        #file_path = f"itsm/chamado_status_historico/year={now:%Y}/month={now:%m}/day={now:%d}/chamado_status_historico_{now:%Y%m%d_%H%M%S}.csv"

        datalake = DataLakeServiceClient.from_connection_string(os.getenv("DATALAKE_CONNECTION_STRING"))
        file_client = datalake.get_file_system_client("bronze").get_file_client(file_path)
        file_client.upload_data(buffer.getvalue().encode("utf-8"), overwrite=True)

        logging.info(f'Arquivo gravado em bronze/{file_path}')

        # ------------------------------------------------------------------
        # 5. Gravação no banco de dados de destino (full refresh)
        # ------------------------------------------------------------------
        # As tabelas de destino não têm constraints; para não duplicar dados a
        # cada execução, faz DELETE + INSERT na mesma transação (rollback em erro).
        col_list = ", ".join(f"[{c}]" for c in columns)
        placeholders = ", ".join("?" for _ in columns)
        insert_sql = f"INSERT INTO itsm.chamado_status_historico ({col_list}) VALUES ({placeholders})"

        dest_conn = pyodbc.connect(dest_conn_str, autocommit=False)
        try:
            dest_cursor = dest_conn.cursor()

            # Verifica se a tabela tem coluna IDENTITY (para manter os ids da origem)
            dest_cursor.execute("SELECT OBJECTPROPERTY(OBJECT_ID('itsm.chamado_status_historico'), 'TableHasIdentity')")
            has_identity = dest_cursor.fetchone()[0] == 1

            dest_cursor.execute("DELETE FROM itsm.chamado_status_historico")

            if has_identity:
                dest_cursor.execute("SET IDENTITY_INSERT itsm.chamado_status_historico ON")

            if rows:
                dest_cursor.fast_executemany = True
                dest_cursor.executemany(insert_sql, [tuple(r) for r in rows])

            if has_identity:
                dest_cursor.execute("SET IDENTITY_INSERT itsm.chamado_status_historico OFF")

            dest_conn.commit()
            logging.info(f'{len(rows)} linhas gravadas em itsm.chamado_status_historico no banco de destino')
        except Exception:
            dest_conn.rollback()
            raise
        finally:
            dest_conn.close()

    except Exception as e:
        logging.error(f"Erro ao processar itsm.chamado_status_historico: {str(e)}")
        raise


# ======================================================================
# itsm.csat_avaliacao
# ======================================================================
@app.timer_trigger(schedule="0 * * * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False)
def extract_csat_avaliacao(myTimer: func.TimerRequest) -> None:
    logging.info('tabela csat_avaliacao')

    # ------------------------------------------------------------------
    # 1. Variáveis de ambiente (configuradas no Function App)
    # ------------------------------------------------------------------
    # Banco de origem
    sql_server = os.getenv("HOST")
    sql_database = os.getenv("DATABASE")
    sql_user = os.getenv("USER")
    sql_pass = os.getenv("PASSWORD")

    # Banco de destino
    dest_server = os.getenv("DEST_HOST")
    dest_database = os.getenv("DEST_DATABASE")
    dest_user = os.getenv("DEST_USER")
    dest_pass = os.getenv("DEST_PASSWORD")

    # ------------------------------------------------------------------
    # 2. Strings de conexão
    # ------------------------------------------------------------------
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={sql_server};"
        f"DATABASE={sql_database};"
        f"UID={sql_user};"
        f"PWD={sql_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    dest_conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={dest_server};"
        f"DATABASE={dest_database};"
        f"UID={dest_user};"
        f"PWD={dest_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    try:
        # ------------------------------------------------------------------
        # 3. Leitura da tabela no banco de origem
        # ------------------------------------------------------------------
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()

            query = "select * from itsm.csat_avaliacao"
            cursor.execute(query)

            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            logging.info(f'{len(rows)} linhas lidas de itsm.csat_avaliacao')

        # ------------------------------------------------------------------
        # 4. Gravação do CSV no contêiner bronze do Data Lake
        # ------------------------------------------------------------------
        # Cria um buffer de memória e escreve cabeçalho + linhas em CSV
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(columns)
        writer.writerows(rows)

        # Caminho particionado por data
        now = datetime.now(timezone.utc)
        file_path = f"csat_avaliacao/{now:%Y/%m/%d}/csat_avaliacao_{now:%Y%m%d_%H%M%S}.csv"

        #itsm/csat_avaliacao/year=2024/month=06/day=05/csat_avaliacao_20240605_123456.csv
        #file_path = f"itsm/csat_avaliacao/year={now:%Y}/month={now:%m}/day={now:%d}/csat_avaliacao_{now:%Y%m%d_%H%M%S}.csv"

        datalake = DataLakeServiceClient.from_connection_string(os.getenv("DATALAKE_CONNECTION_STRING"))
        file_client = datalake.get_file_system_client("bronze").get_file_client(file_path)
        file_client.upload_data(buffer.getvalue().encode("utf-8"), overwrite=True)

        logging.info(f'Arquivo gravado em bronze/{file_path}')

        # ------------------------------------------------------------------
        # 5. Gravação no banco de dados de destino (full refresh)
        # ------------------------------------------------------------------
        # As tabelas de destino não têm constraints; para não duplicar dados a
        # cada execução, faz DELETE + INSERT na mesma transação (rollback em erro).
        col_list = ", ".join(f"[{c}]" for c in columns)
        placeholders = ", ".join("?" for _ in columns)
        insert_sql = f"INSERT INTO itsm.csat_avaliacao ({col_list}) VALUES ({placeholders})"

        dest_conn = pyodbc.connect(dest_conn_str, autocommit=False)
        try:
            dest_cursor = dest_conn.cursor()

            # Verifica se a tabela tem coluna IDENTITY (para manter os ids da origem)
            dest_cursor.execute("SELECT OBJECTPROPERTY(OBJECT_ID('itsm.csat_avaliacao'), 'TableHasIdentity')")
            has_identity = dest_cursor.fetchone()[0] == 1

            dest_cursor.execute("DELETE FROM itsm.csat_avaliacao")

            if has_identity:
                dest_cursor.execute("SET IDENTITY_INSERT itsm.csat_avaliacao ON")

            if rows:
                dest_cursor.fast_executemany = True
                dest_cursor.executemany(insert_sql, [tuple(r) for r in rows])

            if has_identity:
                dest_cursor.execute("SET IDENTITY_INSERT itsm.csat_avaliacao OFF")

            dest_conn.commit()
            logging.info(f'{len(rows)} linhas gravadas em itsm.csat_avaliacao no banco de destino')
        except Exception:
            dest_conn.rollback()
            raise
        finally:
            dest_conn.close()

    except Exception as e:
        logging.error(f"Erro ao processar itsm.csat_avaliacao: {str(e)}")
        raise
