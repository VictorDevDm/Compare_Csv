

gov_naturezas = {1015, 1023, 1244, 1031, 1040, 1058, 1066, 1074, 1082, 1104, 1112, 1120, 1139, 1147, 1155,
                 1163, 1171, 1180, 1198, 1201, 1210, 1228, 1236, 1252, 1260, 1279, 1287, 1295,
                 1309, 1317, 1325, 1333, 2038, 3077}


gov_exceptions_cnpjs = {
    '06981180000116',
    '06981176000158',
    '39244595000166',
    '06067608000110'
}

keywords_gov = ['PREFEITURA', 'GOVERNO', 'CAMARA', 'MUNICIPIO', 'ESTADO', 'SECRETARIA', 'MINISTERIO', 'TRIBUNAL',
                'OUVIDORIA', 'MARINHA', 'EXERCITO', 'AERONAUTICA', 'BANCO CENTRAL', 'RECEITA FEDERAL', 'INSS',
                'IBAMA', 'ANVISA', 'DETRAN', 'CORREIOS', 'IBGE', 'AUTARQUIA', 'SENAI', 'SENAC', 'SESC', 'SESI',
                'SENAR', 'SESCOOP', 'SEST', 'SENAT', 'SEBRAE']


SQL_QUERY_GET_COMPANY_DETAILS = """
    SELECT cnpj_completo, empresa_razao_social, natureza_juridica, capital_social, nome_cnae, municipio, uf, endereco_completo
    FROM company_details
    WHERE cnpj_completo = ANY(%s)
    """

