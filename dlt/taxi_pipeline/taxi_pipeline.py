"""dlt pipeline to load NYC taxi data from the Zoomcamp REST API."""

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig


@dlt.source
def nyc_taxi_rest_api_source() -> dlt.sources.DltSource:
    """Define dlt resources for the NYC taxi REST API."""
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api",
        },
        "resources": [
            {
                "name": "nyc_taxi_trips",
                "endpoint": {
                    # Root path of the Cloud Function
                    "path": "",
                    # The API is paginated by a `page` query parameter and returns
                    # 1,000 records per page. Pagination stops when the API returns
                    # an empty page, which is handled by the default behaviour of
                    # the PageNumberPaginator (`stop_after_empty_page=True`).
                    "paginator": {
                        "type": "page_number",
                        "base_page": 1,
                        # The API does not return a total pages count; instead,
                        # it returns an empty list when there is no more data.
                        # Configure the paginator to stop on empty pages and
                        # not to expect a total_path.
                        "stop_after_empty_page": True,
                        "total_path": None,
                    },
                },
            },
        ],
    }

    yield from rest_api_resources(config)


pipeline = dlt.pipeline(
    pipeline_name="taxi_pipeline",
    destination="duckdb",
    dataset_name="nyc_taxi_data",
    # `refresh="drop_sources"` ensures the data and the state is cleaned
    # on each `pipeline.run()`; remove the argument once you have a
    # working pipeline.
    refresh="drop_sources",
    # Enable dlt progress logs
    progress="log",
)


if __name__ == "__main__":
    load_info = pipeline.run(nyc_taxi_rest_api_source())
    print(load_info)  # noqa: T201

