FROM python:3.10

WORKDIR /usr/src/app

ENV FLASK_APP app.py
ENV FLASK_RUN_HOST 0.0.0.0

RUN curl -sSL https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -o /tmp/miniconda.sh \
    && bash /tmp/miniconda.sh -bfp /usr/local/ \
    && rm -rf /tmp/miniconda.sh \
    && conda install -y python=3 \
    && conda update conda \
    && conda clean --all --yes \
    && conda install -c conda-forge leidenalg -y \
    && conda config --set solver classic \
    && conda install conda-libmamba-solver -y 

COPY requirements.txt ./

RUN pip3 install --no-cache-dir -r requirements.txt
RUN pip3 install -U flask-cors

COPY lib/ ./lib
COPY build ./build
COPY lib.egg-info/ ./lib.egg-info
COPY dist/ ./dist
COPY main.py ./
COPY app.py ./
COPY process_bulk_uploads.py ./
COPY services/ ./services

ENTRYPOINT []

