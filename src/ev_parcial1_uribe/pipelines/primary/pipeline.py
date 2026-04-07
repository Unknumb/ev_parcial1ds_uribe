"""
This is a boilerplate pipeline 'primary'
generated using Kedro 1.3.1
"""
from kedro.pipeline import Pipeline, node, pipeline
from .nodes import crear_tabla_maestra

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=crear_tabla_maestra,
                inputs=[
                    "estudiantes_limpio", 
                    "inscripciones_limpio", 
                    "calificaciones_limpio", 
                    "asistencia_limpio"
                ], # Estos nombres DEBEN ser iguales a los outputs del pipeline de limpieza
                outputs="tabla_maestra_primaria",
                name="crear_master_node"
            )
        ]
    )