import unicodedata
import pandas as pd

#metodos para normalizar texto y nombres de columnas en dataframes
def normalize_text(text):
    if pd.isna(text):
        return None
    text = str(text).strip().lower()
    text = unicodedata.normalize('NFKD', text)
    text = ''.join(c for c in text if not unicodedata.combining(c))
    return text

#metodo para generar una clave  a partir de un texto
def canonical_key(text):
    return normalize_text(text)

#metodo para normalizar nombres de columnas en un dataframe
def normalize_column_name(col):
    col = normalize_text(col)
    col = col.replace(" ", "_")
    return col

#metodo para normalizar los nombres de las columnas de un dataframe
def dataframe_normalize_headers(df):
    df.columns = [normalize_column_name(c) for c in df.columns]
    return df

#metodos para convertir valores a tipos seguros (no breakeabkes)
def safe_int(value):
    try:
        return int(value)
    except:
        return None

def safe_float(value):
    try:
        return float(value)
    except:
        return None

def safe_bool(value):
    if str(value).lower() in ["1", "true", "si", "yes"]:
        return True
    if str(value).lower() in ["0", "false", "no"]:
        return False
    return None