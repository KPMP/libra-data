FROM python:3.10-slim-bullseye

WORKDIR /usr/src/app

ENV FLASK_APP=app.py
ENV FLASK_RUN_HOST=0.0.0.0
ENV CONDA_PLUGINS_AUTO_ACCEPT_TOS=true

RUN apt-get update \
    && apt-get install -y curl

RUN curl -sSL https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -o /tmp/miniconda.sh \
    && bash /tmp/miniconda.sh -bfp /usr/local/ \
    && rm -rf /tmp/miniconda.sh 
RUN conda install --yes python=3.10 \
    && conda update conda \
    && conda clean --all --yes 
RUN conda install -c conda-forge leidenalg --yes \
    && conda config --set solver classic \
    && conda install conda-libmamba-solver --yes 

COPY requirements.txt ./

RUN pip3 config --user set global.progress_bar off
RUN pip3 install --no-cache-dir -r requirements.txt
RUN pip3 install -U flask-cors

COPY lib/ ./lib
COPY main.py ./
COPY app.py ./
COPY process_bulk_uploads.py ./
COPY services/ ./services

ENTRYPOINT ["gunicorn", "-b", ":5000", "app:app", "-t", "600"]

