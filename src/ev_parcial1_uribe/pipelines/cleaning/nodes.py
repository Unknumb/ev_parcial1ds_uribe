"""
This is a boilerplate pipeline 'cleaning'
generated using Kedro 1.3.1
"""

import pandas as pd

def limpiar_estudiantes(df: pd.DataFrame) -> pd.DataFrame:
    """Limpia textos y estandariza el dataset de estudiantes."""
    df_limpio = df.copy()
    
    # Limpiar textos de espacios invisibles y dejar todo en formato Título o Mayúscula
    df_limpio['estado_matricula'] = df_limpio['estado_matricula'].str.strip().str.upper()
    df_limpio['carrera'] = df_limpio['carrera'].str.strip().str.title()
    df_limpio['sede'] = df_limpio['sede'].str.strip().str.title()
    
    # Eliminar duplicados exactos si los hay
    df_limpio = df_limpio.drop_duplicates()
    
    return df_limpio

def limpiar_calificaciones(df: pd.DataFrame) -> pd.DataFrame:
    """Arregla formatos numéricos y elimina notas atípicas."""
    df_limpio = df.copy()
    
    # 1. Forzar notas a números (convierte textos raros a NaN)
    df_limpio['nota'] = pd.to_numeric(df_limpio['nota'], errors='coerce')
    
    # 2. Eliminar las filas donde la nota se volvió nula por ser un texto irreconocible
    df_limpio = df_limpio.dropna(subset=['nota'])
    
    # 3. Eliminar los Outliers (Notas menores a 1.0 o mayores a 7.0 como el famoso 82.1)
    df_limpio = df_limpio[(df_limpio['nota'] >= 1.0) & (df_limpio['nota'] <= 7.0)]
    
    return df_limpio

def limpiar_asistencia(df: pd.DataFrame) -> pd.DataFrame:
    """Estandariza los estados de asistencia."""
    df_limpio = df.copy()
    df_limpio['estado_asistencia'] = df_limpio['estado_asistencia'].str.strip().str.upper()
    return df_limpio

def limpiar_inscripciones(df: pd.DataFrame) -> pd.DataFrame:
    """Limpieza básica de inscripciones."""
    df_limpio = df.copy()
    df_limpio = df_limpio.drop_duplicates()
    return df_limpio