"""
This is a boilerplate pipeline 'reporting'
generated using Kedro 1.3.1
"""
import pandas as pd

def generar_reporte_desercion(df_master: pd.DataFrame) -> pd.DataFrame:
    """Calcula la cantidad de alumnos desertores por sede."""
    
    # Filtramos solo a los desertores
    df_desertores = df_master[df_master['estado_matricula'] == 'DESERTOR']
    
    # Agrupamos por sede y contamos cuántos alumnos únicos hay
    # Usamos nunique() con id_estudiante para no contar dos veces al mismo alumno si tiene varias inscripciones
    reporte = df_desertores.groupby('sede')['id_estudiante'].nunique().reset_index()
    reporte = reporte.rename(columns={'id_estudiante': 'total_desertores'})
    
    # Ordenamos de mayor a menor para que el gráfico se vea bien
    reporte = reporte.sort_values(by='total_desertores', ascending=False)
    
    return reporte

def generar_reporte_rendimiento(df_master: pd.DataFrame) -> pd.DataFrame:
    """Prepara los datos resumidos para analizar la correlación entre ausencias y notas."""
    
    # Agrupamos por inscripción para tener un resumen limpio por ramo
    reporte = df_master.groupby('id_inscripcion').agg(
        total_ausencias=('total_ausencias', 'max'),
        promedio_notas=('promedio_notas', 'max')
    ).reset_index()
    
    return reporte