import csv
from collections import defaultdict
import chardet
from datetime import datetime

"""" --------- ARQUIVO CSV --------- """
"""" CODIGO FILTRA PELO MES SELECIONADO NA LINHA 40 E AGRUPA OS VALORES POR STATUS CONTANDO AS LINHAS """


def detectar_codificacao(arquivo, num_bytes=100000):
    """
    Detecta a codificação de um arquivo lendo os primeiros 'num_bytes' bytes.

    :param arquivo: Caminho para o arquivo.
    :param num_bytes: Número de bytes a serem lidos para detecção.
    :return: Codificação detectada.
    """
    with open(arquivo, 'rb') as f:
        dados = f.read(num_bytes)
        resultado = chardet.detect(dados)
        return resultado['encoding']


def main(input_file):
    # Detectar a codificação lendo os primeiros 100.000 bytes
    codificacao = detectar_codificacao(input_file, num_bytes=100000)
    print(f"Codificação detectada: {codificacao}")

    # Dicionário para armazenar a contagem por status
    contagem_status = defaultdict(int)

    try:
        with open(input_file, mode='r', encoding=codificacao) as file:
            leitor = csv.DictReader(file, delimiter=';')
            for linha in leitor:
                # Verificar e filtrar linhas com data_status no mês 12 de 2024
                data_status = linha.get('data_status', '').strip()
                try:
                    data = datetime.strptime(data_status, '%d/%m/%Y')
                    if data.year == 2025 and data.month == 8:
                        # Agrupar e contar os valores de status
                        status = linha['status'].strip().lower()
                        contagem_status[status] += 1
                except ValueError:
                    # Ignorar linhas com data inválida
                    pass

        # Exibir os resultados
        print("Contagem de linhas por status para o mês de junho de 2025:")
        for status, contagem in contagem_status.items():
            print(f"Status: {status} - Contagem: {contagem}")

    except FileNotFoundError:
        print(f"Erro: O arquivo '{input_file}' não foi encontrado.")
    except KeyError as e:
        print(f"Erro: A coluna {e} não foi encontrada no arquivo.")
    except Exception as e:
        print(f"Ocorreu um erro: {e}")


if __name__ == "__main__":
    main("/home/victor-sims/Desktop/Compare_CSV/data_tim/DATAMOB_6793_Julho.csv")
