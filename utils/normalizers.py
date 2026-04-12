import unicodedata
import pandas as pd

def normalize_text(text):
    if pd.isna(text):
        return None
    text = str(text).strip().lower()
    text = unicodedata.normalize('NFKD', text)
    text = ''.join(c for c in text if not unicodedata.combining(c))
    return text

def canonical_key(text):
    return normalize_text(text)

def normalize_column_name(col):
    col = normalize_text(col)
    col = col.replace(" ", "_")
    return col

def dataframe_normalize_headers(df):
    df.columns = [normalize_column_name(c) for c in df.columns]
    return df

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