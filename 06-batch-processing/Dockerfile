FROM spark:4.0.1-scala2.13-java17-python3-r-ubuntu

USER root

WORKDIR /code

RUN apt-get update && apt-get install -y python3-venv

RUN python3 -m venv /code/.venv

ENV PATH="/code/.venv/bin:$PATH"

RUN pip install --no-cache-dir \
    pandas \
    pyarrow \
    jupyter \
    pyspark

COPY test_spark.py .

ENTRYPOINT [ "python3", "test_spark.py" ]
