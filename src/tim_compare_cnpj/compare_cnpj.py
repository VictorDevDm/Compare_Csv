import pandas as pd
from datetime import date
from pathlib import Path


# -------------------------------
NEW_CSV = "/home/victor-sims/Desktop/Compare_CSV/src/tim_compare_cnpj/data/licencas_unificadas_dezembro.csv"
OLD_CSV = "/home/victor-sims/Desktop/Compare_CSV/src/tim_compare_cnpj/data/licencas_unificadas_novembro.csv"

OUT_EMPRESA_EXISTENTE = "/home/victor-sims/Desktop/Compare_CSV/src/tim_compare_cnpj/data/empresa_existente.csv"
OUT_EMPRESA_NOVA = "/home/victor-sims/Desktop/Compare_CSV/src/tim_compare_cnpj/data/empresa_nova.csv"
OUT_FATURAMENTO_PADRAO = "/home/victor-sims/Desktop/Compare_CSV/src/tim_compare_cnpj/data/faturamento_padrao.csv"
# -------------------------------

COLS = ["MSISDN", "CNPJ", "RAZAO SOCIAL", "DATA_STATUS_SERVICO", "STATUS_SERVICO"]

def read_csv_flex(path: str) -> pd.DataFrame:
    return pd.read_csv(
        path,
        sep=None,
        engine="python",
        dtype=str,
        keep_default_na=False,
        encoding="utf-8"
    )

def normalize_date(s: pd.Series) -> pd.Series:
    s = s.astype(str).str.strip().replace({"": None, "nan": None, "None": None})
    d = pd.to_datetime(s, errors="coerce", dayfirst=False)
    return d.dt.strftime("%Y-%m-%d")

def previous_month_range(ref: date):
    """
    Retorna (inicio_mes_anterior, inicio_mes_atual) como Timestamps.
    Ex: se ref = 2026-02-10 -> [2026-01-01, 2026-02-01)
    """
    first_this_month = date(ref.year, ref.month, 1)

    if ref.month == 1:
        first_prev_month = date(ref.year - 1, 12, 1)
    else:
        first_prev_month = date(ref.year, ref.month - 1, 1)

    print("Mês atual começa em:", first_this_month)
    print("Mês anterior começa em:", first_prev_month)

    return pd.Timestamp(first_prev_month), pd.Timestamp(first_this_month)


def main():
    df_new = read_csv_flex(NEW_CSV)
    df_old = read_csv_flex(OLD_CSV)

    # valida colunas
    for name, df in [("new", df_new), ("old", df_old)]:
        missing = [c for c in COLS if c not in df.columns]
        if missing:
            raise ValueError(f"CSV {name} está sem as colunas: {missing}")

    # normaliza data
    df_new["DATA_STATUS_SERVICO"] = normalize_date(df_new["DATA_STATUS_SERVICO"])
    df_old["DATA_STATUS_SERVICO"] = normalize_date(df_old["DATA_STATUS_SERVICO"])

    start_prev, start_this = previous_month_range(date.today())     # filtra new para mês anterior ao atual
    # start_prev, start_this = previous_month_range(date(2026, 1, 15))

    dt_new = pd.to_datetime(df_new["DATA_STATUS_SERVICO"], errors="coerce")

    df_new_prev_month = df_new.loc[(dt_new >= start_prev) & (dt_new < start_this)].copy()

    old_cnpjs = set(df_old["CNPJ"].astype(str).str.strip())

    # separa existente/nova com base no CNPJ
    df_empresa_existente = df_new_prev_month[df_new_prev_month["CNPJ"].astype(str).str.strip().isin(old_cnpjs)].copy()
    df_empresa_nova = df_new_prev_month[~df_new_prev_month["CNPJ"].astype(str).str.strip().isin(old_cnpjs)].copy()

    # faturamento_padrao:
    key_cols = ["MSISDN", "CNPJ", "DATA_STATUS_SERVICO", "STATUS_SERVICO", "RAZAO SOCIAL"]

    df_new_keyed = df_new.copy()
    df_empresa_nova_keyed = df_empresa_nova.copy()

    df_new_keyed["_k"] = df_new_keyed[key_cols].astype(str).agg("|".join, axis=1)
    df_empresa_nova_keyed["_k"] = df_empresa_nova_keyed[key_cols].astype(str).agg("|".join, axis=1)

    novas_keys = set(df_empresa_nova_keyed["_k"])

    df_faturamento_padrao = df_new_keyed[~df_new_keyed["_k"].isin(novas_keys)].drop(columns=["_k"])

    # garante ordem de colunas
    df_empresa_existente = df_empresa_existente[COLS]
    df_empresa_nova = df_empresa_nova[COLS]
    df_faturamento_padrao = df_faturamento_padrao[COLS]

    df_empresa_existente.to_csv(OUT_EMPRESA_EXISTENTE, index=False, encoding="utf-8")
    df_empresa_nova.to_csv(OUT_EMPRESA_NOVA, index=False, encoding="utf-8")
    df_faturamento_padrao.to_csv(OUT_FATURAMENTO_PADRAO, index=False, encoding="utf-8")

    print(f"OK! Linhas mês anterior (new filtrado): {len(df_new_prev_month)}")
    print(f" - empresa_existente: {len(df_empresa_existente)} -> {Path(OUT_EMPRESA_EXISTENTE).resolve()}")
    print(f" - empresa_nova: {len(df_empresa_nova)} -> {Path(OUT_EMPRESA_NOVA).resolve()}")
    print(f" - faturamento_padrao: {len(df_faturamento_padrao)} -> {Path(OUT_FATURAMENTO_PADRAO).resolve()}")

if __name__ == "__main__":
    main()
