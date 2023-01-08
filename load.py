import logging
from retry import retry
import gspread
import os

logging.basicConfig(
    format="[%(asctime)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S", level=logging.DEBUG
)


@retry(tries=5, delay=5)
def get_spreadsheet():
    credentials_path = os.getenv("TSE_CREDENTIALS_GSPREAD")
    client = gspread.service_account(credentials_path)
    return client.open_by_key(os.getenv("TSE_ID_SPREADSHEET"))


def get_aba(planilha, nome_aba):
    return planilha.worksheet(nome_aba)


def clear_and_load_df(df, aba):
    logging.debug(f"Limpando a aba {aba}")
    aba.clear()
    aba.update([df.columns.values.tolist()] + df.values.tolist())
    logging.info("Upload dos dados feito com sucesso!")
