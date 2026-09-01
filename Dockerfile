# Jupyter image for the Corte 1 data stack.
#
# Base is the official minimal-notebook (Python 3.12). The pyspark variant is
# deliberately NOT used: there is no Spark in this stack, and it would add
# several GB to the image for nothing.
FROM quay.io/jupyter/minimal-notebook:python-3.12

COPY requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    && fix-permissions "${CONDA_DIR}" \
    && fix-permissions "/home/${NB_USER}"

WORKDIR /home/jovyan
