import csv
import io
import logging
import os
from datetime import datetime, timezone

import pyodbc
import azure.functions as func
from azure.storage.filedatalake import DataLakeServiceClient

app = func.FunctionApp()

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
