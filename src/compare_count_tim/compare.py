import csv
from collections import defaultdict
import chardet

"""" --------- ARQUIVO TXT --------- """
"""" CODIGO FILTRA PELO STATUS SELECIONADO NA LINHA 34 E AGRUPA OS VALORES CONTANDO AS LINHAS """

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

    # Dicionário para armazenar a contagem por sncode
    contagem_sncode = defaultdict(int)

    try:
        with open(input_file, mode='r', encoding=codificacao) as file:
            leitor = csv.DictReader(file, delimiter=';')
            for linha in leitor:
                # Verifica se o status é 'a' (ativo), ignorando espaços e maiúsculas/minúsculas
                if linha['status'].strip().lower() == 'a':
                    sncode = linha['sncode']
                    contagem_sncode[sncode] += 1

        # Exibir os resultados
        print("Contagem de linhas por sncode com status 'a':")
        for sncode, contagem in contagem_sncode.items():
            print(f"SNCode: {sncode} - Contagem: {contagem}")

    except FileNotFoundError:
        print(f"Erro: O arquivo '{input_file}' não foi encontrado.")
    except KeyError as e:
        print(f"Erro: A coluna {e} não foi encontrada no arquivo.")
    except Exception as e:
        print(f"Ocorreu um erro: {e}")

if __name__ == "__main__":
    main("/home/victor-sims/Desktop/Compare_CSV/data_tim/txt/DATAMOB_6793_Mai_25.csv")

