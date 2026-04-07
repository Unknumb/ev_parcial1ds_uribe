"""
This is a boilerplate pipeline 'primary'
generated using Kedro 1.3.1
"""
import pandas as pd

def crear_tabla_maestra(df_estudiantes: pd.DataFrame, df_inscripciones: pd.DataFrame, 
                        df_calificaciones: pd.DataFrame, df_asistencia: pd.DataFrame) -> pd.DataFrame:
    """Une los datasets limpios creando una vista analítica maestra."""
    
    # 1. Resumir Asistencia (Usando la lógica que descubrimos en el EDA)
    df_asistencia['es_ausente'] = (df_asistencia['estado_asistencia'] == 'AUSENTE').astype(int)
    resumen_asistencia = df_asistencia.groupby('id_inscripcion')['es_ausente'].sum().reset_index(name='total_ausencias')
    
    # 2. Resumir Calificaciones (Promedio por inscripción)
    resumen_notas = df_calificaciones.groupby('id_inscripcion')['nota'].mean().reset_index(name='promedio_notas')
    
    # 3. Los JOINS (El cruce de la información)
    # Unimos estudiantes con sus inscripciones
    df_master = df_estudiantes.merge(df_inscripciones, on='id_estudiante', how='inner')
    
    # Le pegamos el resumen de ausencias (how='left' por si un alumno aún no tiene clases registradas)
    df_master = df_master.merge(resumen_asistencia, on='id_inscripcion', how='left')
    
    # Le pegamos el promedio de notas
    df_master = df_master.merge(resumen_notas, on='id_inscripcion', how='left')
    
    # 4. Limpieza final de la tabla maestra
    # Si alguien no tenía asistencia registrada, su total de ausencias es 0
    df_master['total_ausencias'] = df_master['total_ausencias'].fillna(0)
    
    return df_master