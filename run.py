from extract_transform import *
from load import *

logging.basicConfig(
    format="[%(asctime)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S", level=logging.DEBUG
)

def main():
    pastas_setup()

    planilha = get_spreadsheet()

    df_dict = {}

    for ano in 2022, 2018:
        df = transformacoes(df=download_arquivo_to_df(ano))

        for turno in 1, 2:
            logging.info(f"{turno}º turno")

            df_turno = df.loc[lambda df: df["NR_TURNO"] == turno]
            df_turno = df_turno[
                [
                    "SG_UF",
                    "NM_UE",
                    "NM_MUNICIPIO",
                    "REGIAO",
                    "NR_VOTAVEL",
                    "NM_VOTAVEL",
                    "QT_VOTOS",
                ]
            ]

            df_turno = agrupamentos(df_turno)

            nome_aba = f"{'primeiro' if turno == 1 else 'segundo'}_turno_{ano}"
            aba = get_aba(
                planilha,
                nome_aba=nome_aba,
            )

            clear_and_load_df(df_turno, aba)

            df_dict[nome_aba] = df_turno



if __name__ == "__main__":
    main()
