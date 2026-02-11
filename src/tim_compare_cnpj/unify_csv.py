import pandas as pd
from pathlib import Path


# ------------------------------------------
CSV_ATIVAS = "/home/victor-sims/Desktop/Compare_CSV/src/tim_compare_cnpj/data/ativas_dezembro.csv"
CSV_CANCEL_SUSP = "/home/victor-sims/Desktop/Compare_CSV/src/tim_compare_cnpj/data/canceladas_dezembro.csv"
SAIDA = "/home/victor-sims/Desktop/Compare_CSV/src/tim_compare_cnpj/data/licencas_unificadas_dezembro.csv"
# ------------------------------------------

COLS_FINAIS = ["MSISDN", "CNPJ", "RAZAO SOCIAL", "DATA_STATUS_SERVICO", "STATUS_SERVICO"]


def read_csv_flex(path: str) -> pd.DataFrame:
    """
    Lê CSV tentando autodetectar separador (, ; \t).
    Força colunas como string para evitar problemas com MSISDN/CNPJ.
    """
    return pd.read_csv(
        path,
        sep=None,              # autodetect
        engine="python",
        dtype=str,
        keep_default_na=False, # evita virar NaN em campos vazios
        encoding="utf-8"
    )


def normalize_cnpj_series(s: pd.Series) -> pd.Series:
    """
    Normaliza CNPJ garantindo 14 dígitos:
    - remove caracteres não numéricos
    - preenche zeros à esquerda
    Ex:
      '01234567000189' -> '01234567000189'
      '1234567000189'  -> '01234567000189'
      '11.084.060/0001-56' -> '11084060000156'
    """
    s = s.astype(str).str.strip()

    # remove tudo que não for número
    s = s.str.replace(r"\D", "", regex=True)

    # garante 14 dígitos preenchendo com zeros à esquerda
    s = s.str.zfill(14)

    return s


def normalize_date_series(s: pd.Series) -> pd.Series:
    """
    Normaliza datas para YYYY-MM-DD (sem horário).
    Assume padrão month-first (M/D/YYYY), pois:
      - Exemplo do parceiro: 2/5/2019 11:18
      - Exemplo status: 12/16/2025
    """
    s = s.astype(str).str.strip()
    s = s.replace({"": None, "nan": None, "None": None})

    d = pd.to_datetime(s, errors="coerce", dayfirst=False)  # sem infer_datetime_format
    d = d.dt.normalize()
    return d.dt.strftime("%Y-%m-%d")


def extract_activation_date_from_history(hist: pd.Series) -> pd.Series:
    """
    Extrai a data de ativação do HISTORICO_SERVICO.

    Regras:
    - Pega sempre o primeiro evento (antes do primeiro '|').
      Ex: '240226a|251216s' -> '240226a'
          '190828a|200318s|...' -> '190828a'
    - Remove o sufixo de status (letra final) e usa só YYMMDD.
    - Converte YYMMDD para YYYY-MM-DD.
      (Assumindo anos 00-69 => 2000-2069 e 70-99 => 1970-1999, padrão do pandas %y)
    """
    hist = hist.astype(str).str.strip().replace({"": None, "nan": None, "None": None})

    # primeiro evento
    first_event = hist.str.split("|", n=1).str[0]

    # pega apenas os 6 dígitos iniciais YYMMDD (ignora a letra final: a/s/d etc.)
    yymmdd = first_event.str.extract(r"(\d{6})", expand=False)

    # converte para data
    dt = pd.to_datetime(yymmdd, format="%y%m%d", errors="coerce")
    return dt.dt.strftime("%Y-%m-%d")


def build_ativas(df: pd.DataFrame) -> pd.DataFrame:
    needed = ["MSISDN", "CNPJ", "RAZAO SOCIAL", "PARCEIRO", "STATUS_SERVICO"]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise ValueError(f"CSV ATIVAS está sem as colunas: {missing}")

    out = df[needed].copy()
    out = out.rename(columns={"PARCEIRO": "DATA_STATUS_SERVICO"})
    out["DATA_STATUS_SERVICO"] = normalize_date_series(out["DATA_STATUS_SERVICO"])
    return out[COLS_FINAIS]


def build_cancel_susp(df: pd.DataFrame) -> pd.DataFrame:
    needed = ["NUM_TERM", "CPF/CNPJ", "RAZAO_SOCIAL", "HISTORICO_SERVICO", "STATUS_SERVICO"]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise ValueError(f"CSV CANCEL/SUSP está sem as colunas: {missing}")

    out = df[needed].copy()
    out = out.rename(columns={
        "NUM_TERM": "MSISDN",
        "CPF/CNPJ": "CNPJ",
        "RAZAO_SOCIAL": "RAZAO SOCIAL",
    })

    # Data de ativação extraída do histórico
    out["DATA_STATUS_SERVICO"] = extract_activation_date_from_history(out["HISTORICO_SERVICO"])

    return out[COLS_FINAIS]


def main():
    df_ativas = read_csv_flex(CSV_ATIVAS)
    df_cancel_susp = read_csv_flex(CSV_CANCEL_SUSP)

    unif = pd.concat(
        [build_ativas(df_ativas), build_cancel_susp(df_cancel_susp)],
        ignore_index=True
    )

    unif["MSISDN"] = unif["MSISDN"].astype(str).str.strip()

    # NORMALIZAÇÃO IMPORTANTE DO CNPJ
    unif["CNPJ"] = normalize_cnpj_series(unif["CNPJ"])

    unif["RAZAO SOCIAL"] = unif["RAZAO SOCIAL"].astype(str).str.strip()
    unif["STATUS_SERVICO"] = unif["STATUS_SERVICO"].astype(str).str.strip()

    # remove duplicatas exatas
    unif = unif.drop_duplicates()

    unif.to_csv(SAIDA, index=False, encoding="utf-8")
    print(f"OK! Gerado: {Path(SAIDA).resolve()} | Linhas: {len(unif)}")


if __name__ == "__main__":
    main()
