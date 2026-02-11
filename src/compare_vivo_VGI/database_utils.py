import logging
import psycopg2


def connect_to_db():
    """
    Conecta ao banco de dados PostgreSQL usando as credenciais definidas nas variáveis de ambiente.

    Returns:
        psycopg2.extensions.connection: Objeto de conexão com o banco de dados.
    """

    dbname = "datamob"
    user = "postgres"
    password = "senha"
    host = "localhost"
    port = "6432"

    try:
        connection = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        return connection
    except Exception as e:
        logging.error(f"Erro ao conectar ao banco de dados: {e}")
        return None