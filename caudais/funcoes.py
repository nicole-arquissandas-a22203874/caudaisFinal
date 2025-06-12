import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import calendar

from .models import Medicao, MedicaoProcessada,EstatisticaMensal,EstatisticaAnual

def carregar_excel(arquivo_excel, serie):
    # Read the Excel file into a DataFrame
    df = pd.read_excel(arquivo_excel)

    # Convert 'Data' column to datetime, invalid dates become NaT (null)
    df['Data'] = pd.to_datetime(df['Data'], format='%d/%m/%Y %H:%M:%S', errors='coerce')

    # Convert 'Caudal' column to numeric, invalid values become NaN (null)
    df['Caudal'] = pd.to_numeric(df['Caudal'], errors='coerce')

    # Create Medicao instances from the DataFrame, allowing nulls for invalid data
    medicoes_to_create = [
        Medicao(
            serie=serie,
            valor=row['Caudal'] if pd.notna(row['Caudal']) else None,  # Replace NaN with None
            timestamp=row['Data'] if pd.notna(row['Data']) else None   # Replace NaT with None
        )
        for _, row in df.iterrows()
    ]

    # Bulk insert Medicao instances into the database
    if medicoes_to_create:
        Medicao.objects.bulk_create(medicoes_to_create, batch_size=7000)

    return 'Medições carregadas com sucesso, incluindo valores nulos para dados inválidos!'
def guardaProcessados(data, metodo,serie):
    to_save = [
        MedicaoProcessada(
            
            serie=serie,
            metodo=metodo,
            timestamp=pd.to_datetime(ts),
            valor=val if pd.notnull(val) else None,
            ano=pd.to_datetime(ts).year
        )
        for ts, val in data
    ]
    MedicaoProcessada.objects.bulk_create(to_save, ignore_conflicts=True)
def guardaEstatisticaAnual(data, metodo, serie):
    to_save = [
        EstatisticaAnual(
           
            serie=serie,
            metodo=metodo,
            ano=ano,
            total=tot if pd.notnull(tot) else 0,
            contagem=cnt if pd.notnull(cnt) else 0,
            media=avg if pd.notnull(avg) else 0
        )
        for ano, tot, cnt, avg in data
    ]
    EstatisticaAnual.objects.bulk_create(to_save, ignore_conflicts=True)
def guardaEstatisticaMensal(data, metodo, serie,selected_year):
    to_save = [
    EstatisticaMensal(
        
        serie=serie,
        metodo=metodo,
        ano=selected_year,
        mes=mes,
        total=tot if pd.notnull(tot) else 0,
        contagem=cnt if pd.notnull(cnt) else 0,
        media=avg if pd.notnull(avg) else 0,
        minWhisker=min_val if pd.notnull(min_val) else 0,
        maxWhisker=max_val if pd.notnull(max_val) else 0,
        medianaMensal=median_val if pd.notnull(median_val) else 0,
        q1=q1_val if pd.notnull(q1_val) else 0,
        q3=q3_val if pd.notnull(q3_val) else 0
    )
    for mes, tot, cnt, avg, min_val, max_val, median_val, q1_val, q3_val in data
]
    EstatisticaMensal.objects.bulk_create(to_save, ignore_conflicts=True)

def normalize(original_df,resampled_df, time):
    for col in resampled_df.columns:

        missing_mask = resampled_df[col].isnull()

        for idx, value in resampled_df[col][missing_mask].items():
          idx = pd.Timestamp(idx)
          before_idx = original_df.loc[:idx, col].last_valid_index()
          after_idx = original_df.loc[idx:, col].first_valid_index()
          if before_idx is not None and after_idx is not None and (after_idx - before_idx).total_seconds() <= pd.Timedelta(minutes=time).total_seconds():
            resampled_df.at[idx, col] = original_df[col][before_idx] + \
                                            ((original_df[col][after_idx] - original_df[col][before_idx]) /
                                            (after_idx - before_idx).total_seconds()) * \
                                            (idx - before_idx).total_seconds()
            