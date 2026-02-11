import logging
import pandas as pd
from src.compare_tim.database_utils import connect_to_db
from src.compare_tim.config import gov_naturezas, gov_exceptions_cnpjs

""" USA APENAS A NATUREZA JURIDICA PARA PODER FAZER A CLASSIFICACAO """

def fetch_company_details(cnpjs, batch_size=1000):
    """
    Busca detalhes da empresa no banco de dados para uma lista de CNPJs, em lotes.

    Args:
    - cnpjs (list): Lista de CNPJs para buscar os detalhes.
    - batch_size (int): Tamanho do lote para as consultas.

    Returns:
    - dict: Dicionário com detalhes da empresa, indexado pelo CNPJ.
    """
    cnpjs = [cnpj.strip().zfill(14) for cnpj in cnpjs if cnpj and cnpj.strip()]

    if not cnpjs:
        logging.error("A lista de CNPJs está vazia após a limpeza.")
        return {}

    company_details = {}

    try:
        with connect_to_db() as conn:
            with conn.cursor() as cursor:

                # Divide a lista de CNPJs em lotes
                for i in range(0, len(cnpjs), batch_size):
                    batch = cnpjs[i:i + batch_size]

                    # Executa a consulta SQL para cada lote
                    query = """
                        SELECT cnpj_completo, natureza_juridica
                        FROM company_details
                        WHERE cnpj_completo IN %s;
                    """
                    cursor.execute(query, (tuple(batch),))
                    rows = cursor.fetchall()

                    # Mapeia os resultados para um dicionário
                    for row in rows:
                        company_details[row[0]] = {
                            'natureza_juridica': row[1]
                        }

    except Exception as e:
        logging.error(f"Error fetching company details: {e}")
        return {}

    return company_details


def add_company_details(df):
    """
    Adiciona detalhes da empresa ao DataFrame com base nos CNPJs e classifica as empresas.

    Args:
    - df (pd.DataFrame): O DataFrame contendo os dados a serem atualizados.
    - gov_naturezas (set): Conjunto de códigos de natureza jurídica que correspondem ao governo.
    - gov_exceptions_cnpjs (set): Conjunto de CNPJs que são exceções e devem ser classificados como governo.

    Returns:
    - pd.DataFrame: O DataFrame atualizado com detalhes da empresa adicionados e classificações.
    """
    # Limpa e ajusta os CNPJs (CUST_ID_TEXT)
    df['CNPJ'] = df['CNPJ'].apply(lambda x: str(x).strip().zfill(14) if pd.notnull(x) else '')

    # Filtra empresas com CNPJ válido
    df_com_cnpj = df[df['CNPJ'] != '']
    cnpjs = df_com_cnpj['CNPJ'].unique().tolist()

    # Busca detalhes das empresas
    company_details = fetch_company_details(cnpjs)

    # Atualiza o DataFrame com os detalhes da empresa
    df['natureza_juridica'] = df['CNPJ'].map(lambda cnpj: company_details.get(cnpj, {}).get('natureza_juridica', None))

    # Inicializa 'tipo_empresa' como 'PRIVADO'
    df['tipo_empresa'] = 'PRIVADO'

    # Classifica como 'GOVERNO' com base na natureza jurídica
    mask_natureza = df['natureza_juridica'].apply(lambda x: pd.notnull(x) and str(x).isdigit() and int(x) in gov_naturezas)
    df.loc[mask_natureza, 'tipo_empresa'] = 'GOVERNO'

    # Classifica como 'GOVERNO' se o CNPJ estiver em gov_exceptions_cnpjs
    mask_cnpj = df['CNPJ'].isin(gov_exceptions_cnpjs)
    df.loc[mask_cnpj, 'tipo_empresa'] = 'GOVERNO'

    return df


def process_csv(input_csv, output_csv):
    """
    Processa o arquivo CSV de entrada, classifica as empresas e salva o resultado em um novo arquivo CSV.

    Args:
    - input_csv (str): Caminho para o arquivo CSV de entrada.
    - output_csv (str): Caminho para salvar o arquivo CSV de saída.
    """
    # Lê o arquivo CSV
    df = pd.read_csv(input_csv)

    # Adiciona os detalhes e classificações
    df = add_company_details(df)

    # Salva o DataFrame resultante em um novo CSV
    df.to_csv(output_csv, index=False)
    print(f"Arquivo salvo com sucesso em: {output_csv}")


# Exemplo de chamada da função
if __name__ == "__main__":
    input_csv = '/home/victor-sims/Desktop/Compare_CSV/data/tim/TIM_ativos_12_25.csv'
    output_csv = '/home/victor-sims/Desktop/Compare_CSV/data/tim/TIM_ativos_12_25.csv_Classificado.csv'
    process_csv(input_csv, output_csv)
