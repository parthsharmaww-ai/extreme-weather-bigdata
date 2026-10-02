FROM quay.io/jupyter/pyspark-notebook@sha256:117fff1aa365055861f109057111a5846701e550ff8c953ac0ca4597b6dff423

COPY requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt
