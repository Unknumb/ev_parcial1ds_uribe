"""
This is a boilerplate pipeline 'reporting'
generated using Kedro 1.3.1
"""
from kedro.pipeline import Pipeline, node, pipeline
from .nodes import generar_reporte_desercion, generar_reporte_rendimiento

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=generar_reporte_desercion,
                inputs="tabla_maestra_primaria", # Entra la tabla del Paso 3
                outputs="reporte_desercion",     # Sale el resumen 1
                name="reporte_desercion_node"
            ),
            node(
                func=generar_reporte_rendimiento,
                inputs="tabla_maestra_primaria", # Entra la misma tabla del Paso 3
                outputs="reporte_rendimiento",   # Sale el resumen 2
                name="reporte_rendimiento_node"
            )
        ]
    )