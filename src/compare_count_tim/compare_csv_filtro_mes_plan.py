import csv
from collections import defaultdict
import chardet
from datetime import datetime
from typing import Dict

""" --------- ARQUIVO CSV --------- """
""" CÓDIGO FILTRA PELO MÊS/ANO DEFINIDOS E:
    1) Conta a quantidade total POR PLANO (coluna 'sncode')
    2) Dentro de cada PLANO, conta a quantidade POR STATUS (ativas/desativadas ou o que vier em 'status')
"""


def detectar_codificacao(arquivo: str, num_bytes: int = 100_000) -> str:
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


def normalizar_status(status_raw: str) -> str:
    """Normaliza o status para facilitar a leitura.
    Mapeia variações comuns de ativo/inativo.
    Mantém o valor original se não entrar em nenhum caso.
    """
    s = (status_raw or '').strip().lower()
    ativos = {"ativo", "ativa", "active", "ativada"}
    inativos = {"inativo", "inativa", "inactive", "desativado", "desativada", "desativa", "desativada(o)"}

    if s in ativos:
        return "ativa"
    if s in inativos:
        return "desativada"
    return s or "indefinido"


def main(input_file: str, ano: int = 2025, mes: int = 6) -> None:
    # Detectar a codificação lendo os primeiros 100.000 bytes
    codificacao = detectar_codificacao(input_file, num_bytes=100_000)
    print(f"Codificação detectada: {codificacao}")

    # Contagens
    contagem_por_plano: Dict[str, int] = defaultdict(int)
    contagem_por_plano_status: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    contagem_status_geral: Dict[str, int] = defaultdict(int)

    try:
        with open(input_file, mode='r', encoding=codificacao) as file:
            leitor = csv.DictReader(file, delimiter=';')
            for linha in leitor:
                # Filtrar pelo mês/ano desejados
                data_status = (linha.get('data_status') or '').strip()
                try:
                    data = datetime.strptime(data_status, '%d/%m/%Y')
                except ValueError:
                    # Ignorar linhas com data inválida
                    continue

                if data.year == ano and data.month == mes:
                    plano = (linha.get('sncode') or '').strip() or 'SEM_PLANO'
                    status_raw = (linha.get('status') or '').strip()
                    status = normalizar_status(status_raw)

                    contagem_por_plano[plano] += 1
                    contagem_por_plano_status[plano][status] += 1
                    contagem_status_geral[status] += 1

        # Exibir os resultados
        nome_mes = {
            1: 'janeiro', 2: 'fevereiro', 3: 'março', 4: 'abril', 5: 'maio', 6: 'junho',
            7: 'julho', 8: 'agosto', 9: 'setembro', 10: 'outubro', 11: 'novembro', 12: 'dezembro'
        }.get(mes, str(mes))

        print(f"\nContagem por PLANO (sncode) para {nome_mes} de {ano}:")
        # Ordena planos por total (decrescente) e depois alfabeticamente
        for plano in sorted(contagem_por_plano.keys(), key=lambda p: (-contagem_por_plano[p], p)):
            total_plano = contagem_por_plano[plano]
            print(f"\nPlano: {plano} — Total: {total_plano}")
            # Ordena status por contagem (decrescente)
            for status in sorted(contagem_por_plano_status[plano].keys(), key=lambda s: (-contagem_por_plano_status[plano][s], s)):
                print(f"  {status}: {contagem_por_plano_status[plano][status]}")

        # Resumo geral por status
        print(f"\nResumo GERAL por status em {nome_mes}/{ano}:")
        for status in sorted(contagem_status_geral.keys(), key=lambda s: (-contagem_status_geral[s], s)):
            print(f"  {status}: {contagem_status_geral[status]}")

    except FileNotFoundError:
        print(f"Erro: O arquivo '{input_file}' não foi encontrado.")
    except KeyError as e:
        print(f"Erro: A coluna {e} não foi encontrada no arquivo.")
    except Exception as e:
        print(f"Ocorreu um erro: {e}")


if __name__ == "__main__":
    # Ajuste o caminho e o filtro de data conforme necessário
    main("/home/victor-sims/Desktop/Compare_CSV/data_tim/DATAMOB_6793_Mai_25.csv", ano=2025, mes=6)
