import great_expectations as gx
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.batch import RuntimeBatchRequest
import pandas as pd

# Contexto del proyecto
context = gx.get_context(context_root_dir="great_expectations")

def create_or_overwrite_suite(suite_name: str, expectations: list):
    # Crea o actualiza la suite
    suite = gx.core.ExpectationSuite(expectation_suite_name=suite_name)
    context.add_or_update_expectation_suite(expectation_suite=suite)

    # Dummy DataFrame solo para asociar expectativas
    if suite_name == "cleaned_api_suite":
        dummy_df = pd.DataFrame(columns=["latitude", "longitude", "category_group"])
    else:
        dummy_df = pd.DataFrame(columns=["lat", "long", "neighbourhood_group", "room_type", "review_rate_number", "price"])


    batch_request = RuntimeBatchRequest(
        datasource_name="runtime_pandas_datasource",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name=suite_name,
        runtime_parameters={"batch_data": dummy_df},
        batch_identifiers={"default_identifier_name": "default_identifier"},
    )

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite=suite
    )

    for exp in expectations:
        method = getattr(validator, exp["expectation_type"])
        method(**exp["kwargs"])

    context.save_expectation_suite(validator.get_expectation_suite())
    print(f"✅ Suite '{suite_name}' creada con {len(expectations)} expectativas.")

# --------- cleaned_api_suite ---------
expectations_api = [
    {"expectation_type": "expect_column_to_exist", "kwargs": {"column": "latitude"}},
    {"expectation_type": "expect_column_to_exist", "kwargs": {"column": "longitude"}},
    {"expectation_type": "expect_column_values_to_be_between", "kwargs": {"column": "latitude", "min_value": -90, "max_value": 90}},
    {"expectation_type": "expect_column_values_to_be_between", "kwargs": {"column": "longitude", "min_value": -180, "max_value": 180}},
    {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "category_group"}},
    {"expectation_type": "expect_column_values_to_be_in_set", "kwargs": {
        "column": "category_group",
        "value_set": [
            "cultural", "restaurants", "parks_&_outdoor", "retail_&_shopping",
            "entertainment_&_leisure", "landmarks", "bars_&_clubs", "Other"
        ]
    }},
]

# --------- cleaned_airbnb_suite ---------
expectations_airbnb = [
    {"expectation_type": "expect_column_values_to_be_between", "kwargs": {"column": "lat", "min_value": -90, "max_value": 90}},
    {"expectation_type": "expect_column_values_to_be_between", "kwargs": {"column": "long", "min_value": -180, "max_value": 180}},
    {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "neighbourhood_group"}},
    {"expectation_type": "expect_column_values_to_be_in_set", "kwargs": {"column": "room_type", "value_set": ["Private room", "Entire home/apt", "Shared room", "Hotel room"]}},
    {"expectation_type": "expect_column_values_to_be_between", "kwargs": {"column": "review_rate_number", "min_value": 0, "max_value": 5}},
    {"expectation_type": "expect_column_values_to_be_between", "kwargs": {"column": "price", "min_value": 10, "max_value": 10000}},
]

# Crear ambas suites
create_or_overwrite_suite("cleaned_api_suite", expectations_api)
create_or_overwrite_suite("cleaned_airbnb_suite", expectations_airbnb)
