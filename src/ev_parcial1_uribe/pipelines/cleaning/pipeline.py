"""
This is a boilerplate pipeline 'cleaning'
generated using Kedro 1.3.1
"""


from kedro.pipeline import Pipeline, node, pipeline
from .nodes import limpiar_estudiantes, limpiar_calificaciones, limpiar_asistencia, limpiar_inscripciones

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=limpiar_estudiantes,
                inputs="estudiantes_raw",
                outputs="estudiantes_limpio",
                name="limpiar_estudiantes_node"
            ),
            node(
                func=limpiar_calificaciones,
                inputs="calificaciones_raw",
                outputs="calificaciones_limpio",
                name="limpiar_calificaciones_node"
            ),
            node(
                func=limpiar_asistencia,
                inputs="asistencia_raw",
                outputs="asistencia_limpio",
                name="limpiar_asistencia_node"
            ),
            node(
                func=limpiar_inscripciones,
                inputs="inscripciones_raw",
                outputs="inscripciones_limpio",
                name="limpiar_inscripciones_node"
            )
        ]
    )