"""Pipeline to ingest data from the Open Library Search API."""

from dlt.sources.rest_api import rest_api_source

import dlt


def open_library_source(query: str = "harry potter"):
    """
    Create a dlt source for the Open Library Search API.

    Args:
        query: Search query string (default: "harry potter")
    """
    return rest_api_source(
        {
            "client": {
                "base_url": "https://openlibrary.org",
            },
            "resource_defaults": {
                "primary_key": "key",
                "write_disposition": "replace",
            },
            "resources": [
                {
                    "name": "books",
                    "endpoint": {
                        "path": "search.json",
                        "params": {
                            "q": query,
                            "limit": 100,
                        },
                        "data_selector": "docs",
                        "paginator": {
                            "type": "offset",
                            "limit": 100,
                            "offset_param": "offset",
                            "limit_param": "limit",
                            "total_path": "numFound",
                        },
                    },
                },
            ],
        }
    )


pipeline = dlt.pipeline(
    pipeline_name="open_library_pipeline",
    destination="duckdb",
    dataset_name="open_library_data",
    progress="log",  # logs the pipeline run (Optional)
)

load_info = pipeline.run(open_library_source())


# All the logic below will be run automatically under the hood after .run
"""
# Extract: download raw data from the API
# which resources were extracted
# which tables will be created later
# how many rows were extracted per resource
extract_info = pipeline.extract(open_library_source())
load_id = extract_info.loads_ids[-1]
m = extract_info.metrics[load_id][0]

print("Resources:", list(m["resource_metrics"].keys()))
print("Tables:", list(m["table_metrics"].keys()))
print("Load ID:", load_id)
print()

for resource, rm in m["resource_metrics"].items():
    print(f"Resource: {resource}")
    print(f"rows extracted: {rm.items_count}")
    print()


# Normalize: turn nested JSON into relational tables
# from nested JSON into a relational structure with multiple linked tables
# dlt maintains relationships using _dlt_id and _dlt_parent_id

# dlt also creates internal tables to track pipeline state:
# _dlt_loads: Tracks load history (when data was loaded, status)
# _dlt_pipeline_state: Stores pipeline state for incremental loading
# _dlt_version: Tracks schema versions

# In context of lirary dlt (data load tool) column _dlt_list_idx is a system field,
# that is saved to keeping the original order during (unnesting) of nested lists in a relational table

normalize_info = pipeline.normalize()
load_id = normalize_info.loads_ids[-1]
m = normalize_info.metrics[load_id][0]

print("Load ID:", load_id)
print()

print("Tables created/updated:")
for table_name, tm in m["table_metrics"].items():
    # skip dlt internal tables to keep it beginner-friendly
    if table_name.startswith("_dlt"):
        continue
    print(f"  - {table_name}: {tm.items_count} rows")

# Creates and saves the schema in json format by default .dlt/pipelines/<pipeline_name>/schemas/
pipeline.default_schema

# Load: write those tables into DuckDB
load_info = pipeline.load()
print(load_info)
"""

# Inspect the loaded data
ds = pipeline.dataset()
print(ds.tables)

df = ds.books.df()
print(df.head(3))

df = ds.books__language.df()
print(df.head(3))
