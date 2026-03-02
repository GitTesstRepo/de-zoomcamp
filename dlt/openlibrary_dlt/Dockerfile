FROM python:3.13.11-slim

WORKDIR /code

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

RUN python -m venv /code/.venv

ENV PATH="/code/.venv/bin:$PATH"

# RUN pip -q install dlt[duckdb]

# dlt uses numpy under the hood and pandas when we use dataframes
RUN uv pip install "dlt[duckdb]" numpy pandas

COPY openlibrary_dlt.py .

ENTRYPOINT [ "python", "openlibrary_dlt.py" ]