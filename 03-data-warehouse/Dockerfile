FROM python:3.13.11-slim

WORKDIR /code

RUN python -m venv /code/.venv

ENV PATH="/code/.venv/bin:$PATH"

RUN pip install --no-cache-dir google-cloud-storage

COPY load_yellow_taxi_data.py .

ENTRYPOINT [ "python", "load_yellow_taxi_data.py" ]