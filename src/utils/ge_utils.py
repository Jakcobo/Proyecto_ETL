import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.exceptions import GreatExpectationsError
#from great_expectations.exceptions import DataContextNotFoundError
import pandas as pd
import logging
import os
import webbrowser
logger = logging.getLogger(__name__)

DEFAULT_GE_CONTEXT_ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "great_expectations"))

 
def validate_dataframe_with_ge(
    df: pd.DataFrame,
    expectation_suite_name: str,
    data_asset_name: str = "my_runtime_asset",
    ge_context_root_dir: str = None
):
    """
    Valida un DataFrame de Pandas usando una Expectation Suite de Great Expectations.

    Args:
        df (pd.DataFrame): El DataFrame a validar.
        expectation_suite_name (str): El nombre de la Expectation Suite a usar.
        data_asset_name (str, optional): Nombre del asset para este batch.
        ge_context_root_dir (str, optional): Ruta al directorio raíz de GE.
                                            Si es None, usa DEFAULT_GE_CONTEXT_ROOT_DIR.

    Returns:
        great_expectations.validation_operators.types.validation_operator_result.ValidationOperatorResult:
        El resultado de la validación. Puedes acceder a .success para ver si pasó.
    """
    if ge_context_root_dir is None:
        ge_context_root_dir = DEFAULT_GE_CONTEXT_ROOT_DIR
        logger.info(f"Usando GE context root dir por defecto: {ge_context_root_dir}")


    try:
        context = gx.get_context(context_root_dir=ge_context_root_dir)
    except GreatExpectationsError:
    #except DataContextNotFoundError:
        logger.error(f"Error: No se pudo encontrar el Data Context de Great Expectations en {ge_context_root_dir}")
        logger.error("Asegúrate de que 'great_expectations init' se haya ejecutado y la ruta sea correcta.")
        raise

    # Usaremos un RuntimeDataConnector para validar DataFrames en memoria
    # Asegúrate de tener una configuración similar en tu great_expectations.yml para 'runtime_pandas_datasource'
    # Ejemplo en great_expectations.yml:
    # datasources:
    #   runtime_pandas_datasource:
    #     class_name: Datasource
    #     execution_engine:
    #       class_name: PandasExecutionEngine
    #     data_connectors:
    #       default_runtime_data_connector_name:
    #         class_name: RuntimeDataConnector
    #         batch_identifiers:
    #           - default_identifier_name

    batch_request = RuntimeBatchRequest(
        datasource_name="runtime_pandas_datasource", # Nombre del datasource para DataFrames en memoria
        data_connector_name="default_runtime_data_connector_name", # Conector por defecto para runtime
        data_asset_name=data_asset_name, # Puedes hacerlo único por task o DataFrame
        runtime_parameters={"batch_data": df},
        batch_identifiers={"default_identifier_name": "default_identifier"}
    )

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=expectation_suite_name
    )
    validation_result = validator.validate()

    if not validation_result.success:
        logger.warning(f"Validación fallida para la suite '{expectation_suite_name}' sobre el asset '{data_asset_name}'.")
        logger.warning(str(validation_result))
    else:
        logger.info(f"Validación exitosa para la suite '{expectation_suite_name}' sobre el asset '{data_asset_name}'.")

    context.save_expectation_suite(expectation_suite=validator.get_expectation_suite(discard_failed_expectations=False))
    context.build_data_docs() # Si quieres reconstruir Data Docs aquí
    # Abrir Data Docs automáticamente si se está ejecutando localmente
        # Abrir Data Docs automáticamente si se está ejecutando localmente
    import subprocess

    import subprocess

    if os.environ.get("AIRFLOW_ENV", "local") == "local":
        docs_path = os.path.abspath(os.path.join(ge_context_root_dir, "uncommitted", "data_docs", "local_site", "index.html"))
        if os.path.exists(docs_path):
            logger.info(f"Abrir Data Docs en navegador: {docs_path}")
            if os.name == "posix" and "microsoft" in os.uname().release.lower():  # WSL
                windows_path = subprocess.check_output(["wslpath", "-w", docs_path]).decode("utf-8").strip()
                cleaned_windows_path = windows_path.replace('\\', '/')
                file_url = f"file:///{cleaned_windows_path}"
                subprocess.Popen(["cmd.exe", "/C", f'start chrome {file_url}'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


            else:
                import webbrowser
                webbrowser.open(f"file://{docs_path}")
        else:
            logger.warning(f"Data Docs no encontrado en: {docs_path}")


    return validation_result