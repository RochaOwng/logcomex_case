import pandas as pd
from typing import Tuple

def load_data() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Load data from CSV files.

    Returns:
        Tuple of DataFrames: Tuple containing DataFrames for various data sources.
    """
    df_imp_municipio_sh4 = pd.read_csv('data/EXP_2023_MUN.csv', sep=';', dtype=str)
    df_imp_df_ncm = pd.read_csv('data/EXP_2023.csv', sep=';', dtype=str)
    df_uf_mun = pd.read_csv('data/UF_MUN.csv', sep=';', encoding='ISO-8859-1', dtype=str)
    df_urf = pd.read_csv('data/URF.csv', sep=';', encoding='ISO-8859-1', dtype=str)
    df_pais = pd.read_csv('data/PAIS.csv', sep=';', encoding='ISO-8859-1', dtype=str)
    df_via = pd.read_csv('data/VIA.csv', sep=';', encoding='ISO-8859-1', dtype=str)
    df_ncm = pd.read_csv('data/NCM.csv', sep=';', encoding='ISO-8859-1', dtype=str)
    return df_imp_municipio_sh4, df_imp_df_ncm, df_uf_mun, df_urf, df_pais, df_via, df_ncm

def preprocess_data(df_imp_df_ncm: pd.DataFrame, df_urf: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Preprocess data by adding new columns to DataFrames.

    Args:
        df_imp_df_ncm (DataFrame): DataFrame with import NCM data.
        df_urf (DataFrame): DataFrame with URF data.

    Returns:
        Tuple of DataFrames: Tuple containing preprocessed DataFrames.
    """
    df_imp_df_ncm['CO_NCM_4'] = df_imp_df_ncm['CO_NCM'].astype(str).str[:4]
    df_urf['NO_URF_2'] = df_urf['NO_URF'].str.split(' - ').str[1]
    return df_imp_df_ncm, df_urf

def merge_data(
    df_imp_df_ncm: pd.DataFrame, 
    df_urf: pd.DataFrame, 
    df_via: pd.DataFrame, 
    df_ncm: pd.DataFrame) -> pd.DataFrame:
    """
    Merge data from different DataFrames.

    Args:
        df_imp_df_ncm (DataFrame): DataFrame with import data and new columns.
        df_urf (DataFrame): DataFrame with URF data.
        df_via (DataFrame): DataFrame with VIA data.
        df_ncm (DataFrame): DataFrame with NCM data.

    Returns:
        DataFrame: Merged DataFrame with combined information.
    """
    df_ncm_with_infos = df_imp_df_ncm.merge(df_urf, on='CO_URF', how='left') \
                                     .merge(df_via, on='CO_VIA', how='left') \
                                     .merge(df_ncm, on='CO_NCM', how='left')
    return df_ncm_with_infos

def create_dataset(
    df_imp_municipio_sh4: pd.DataFrame, 
    df_uf_mun: pd.DataFrame, 
    df_pais: pd.DataFrame, 
    df_ncm_with_infos: pd.DataFrame) -> pd.DataFrame:
    """
    Create the final dataset by merging and selecting columns.

    Args:
        df_imp_municipio_sh4 (DataFrame): DataFrame with import data for municipalities.
        df_uf_mun (DataFrame): DataFrame with UFs and municipalities data.
        df_pais (DataFrame): DataFrame with country data.
        df_ncm_with_infos (DataFrame): DataFrame with combined NCM data.

    Returns:
        DataFrame: Final dataset with selected columns.
    """
    df_imp_municipio_with_infos = df_imp_municipio_sh4.merge(df_uf_mun, left_on='CO_MUN', right_on='CO_MUN_GEO', how='left') \
        .merge(df_pais, on='CO_PAIS', how='left')
    
    df_final = df_imp_municipio_with_infos.merge(df_ncm_with_infos, 
        left_on=['SH4', 'NO_MUN', 'CO_PAIS', 'CO_ANO', 'CO_MES'],
        right_on=['CO_NCM_4', 'NO_URF_2', 'CO_PAIS', 'CO_ANO', 'CO_MES'],
        how='left')
    
    cols_to_select = ['CO_ANO', 'CO_MES', 'SH4', 'CO_NCM', 'NO_PAIS', 
        'SG_UF_MUN', 'NO_MUN', 'NO_URF', 'NO_VIA', 'NO_NCM_POR']

    df_final = df_final[cols_to_select]

    return df_final

if __name__ == "__main__":
    df_imp_municipio_sh4, df_imp_df_ncm, df_uf_mun, df_urf, df_pais, df_via, df_ncm = load_data()
    df_imp_df_ncm, df_urf = preprocess_data(df_imp_df_ncm, df_urf)
    df_ncm_with_infos = merge_data(df_imp_df_ncm, df_urf, df_via, df_ncm)
    df_final = create_dataset(df_imp_municipio_sh4, df_uf_mun, df_pais, df_ncm_with_infos)

    df_final.to_csv('output_dataset.csv', sep=';', index=False)