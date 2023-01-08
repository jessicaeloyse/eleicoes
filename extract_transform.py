import pandas as pd
import os
import requests
from retry import retry
import logging
import zipfile
import io
from datetime import datetime, timedelta

logging.basicConfig(format='[%(asctime)s] %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.DEBUG)


@retry(AssertionError, tries=3, backoff=4, delay=2)
def download_arquivo_to_df(ano):
    pasta = '/arquivos'
    data_min = datetime.timestamp((datetime.today() - timedelta(days=60)))
    nome_arquivo = f'votacao_secao_{ano}_BR'
    nome_arquivo_full = f"{nome_arquivo}.csv"
    url = f'https://cdn.tse.jus.br/estatistica/sead/odsele/votacao_secao/{nome_arquivo}.zip'

    if not os.path.exists(nome_arquivo_full) or os.path.getctime(nome_arquivo_full) <= data_min:

        logging.info(f"Iniciando o download: {url}")

        with requests.get(url) as r:
            assert r.status_code == 200, "Falha no download do arquivo"

            logging.debug(f"Extraindo o zip em : '{pasta}' ...")
            with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                z.extractall(f"{pasta}/")

        logging.info("Download e extração concluídos!")
    else:
        logging.debug(f"Arquivo {nome_arquivo_full} já existe")

    return pd.read_csv(nome_arquivo_full, sep=';', encoding='latin-1')


def transformacoes(df):
    rename_values = ["LULA", "BOLSONARO", "HADDAD", "CIRO", "TEBET",
                     "BOULOS", "DACIOLO", "AMOEDO", "MARINA", "KELMON",
                     "THRONICKE", "D AVILA", "VERA LUCIA", "MANZANO"]

    col = "NM_VOTAVEL"

    logging.debug("Tratando nomes dos candidatos")

    for nome in rename_values:
        df.loc[df[col].str.contains(nome), col] = nome

    regioes = {'Norte': ('AC', 'AP', 'AM', 'PA', 'RO', 'RR', 'TO'),
               'Nordeste': ('AL', 'BA', 'CE', 'MA', 'PB', 'RR', 'PE', 'PI', 'RN', 'SE'),
               'Centro-Oeste': ('DF', 'GO', 'MT', 'MS'),
               'Sudeste': ('ES', 'SP', 'RJ', 'MG'),
               'Sul': ('PR', 'RS', 'SC')}

    for regiao, siglas in regioes.items():
        df.loc[df['SG_UF'].isin(siglas), "REGIAO"] = regiao

    return df


def pastas_setup(lista_pastas=('arquivos',)):
    for nome_pasta in lista_pastas:
        if not os.path.exists(nome_pasta):
            os.mkdir(nome_pasta)
            logging.info(f"Pasta {nome_pasta} criada com sucesso!")


def main():
    pastas_setup()

    for ano in 2022, 2018:
        df = download_arquivo_to_df(ano)

        df = transformacoes(df)

        for turno in 1, 2:
            print(f"{turno}º turno")

            df_turno = df.loc[lambda df: df['NR_TURNO'] == turno]
            df_turno = df_turno[['SG_UF', 'NM_UE', 'NM_MUNICIPIO', 'NR_VOTAVEL', 'NM_VOTAVEL', 'QT_VOTOS']]

            df_turno = df_turno.groupby(["SG_UF", "NM_UE", "NM_MUNICIPIO", "NR_VOTAVEL", "NM_VOTAVEL"],
                                        as_index=False).sum("QT_VOTOS")

            csv_path = f"{'primeiro' if turno == 1 else 'segundo'}_turno_{ano}.csv"
            df_turno = df_turno.loc[lambda df: df['SG_UF'] != 'ZZ']

            for col in 'NM_MUNICIPIO', 'SG_UF', 'REGIAO':
                df_vencedor = df_turno.groupby([col, 'NM_VOTAVEL'], as_index=False)['QT_VOTOS'].sum()

                df_vencedor['RN'] = df_vencedor.sort_values('QT_VOTOS', ascending=[False]) \
                                        .groupby([col]) \
                                        .cumcount() + 1

                df_vencedor = df_vencedor[df_vencedor['RN'] == 1]

                df_vencedor = df_vencedor.rename(columns={"NM_VOTAVEL": f'VENCEDOR{col[2:]}',
                                                          "QT_VOTOS": f"VENCEDOR_VOTOS_POR{col[2:]}"})

                del (df_vencedor["RN"])

                df_turno = pd.merge(left=df_turno, right=df_vencedor, how='left', on=col)

            df_turno.to_csv(csv_path, index=False)
            print(f"Saved: {csv_path}")


if __name__ == '__main__':
    main()
