FROM rocker/r-base:latest

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    python3 python3-dev python3-pip python3-venv \
    build-essential libffi-dev libssl-dev \
    libxml2-dev libcurl4-openssl-dev \
    libbz2-dev libzstd-dev liblzma-dev \
    libfreetype6-dev libpng-dev libjpeg-dev \
    libopenblas-dev gfortran \
    git \
    curl \
    && apt-get clean

# Cria e ativa ambiente virtual Python
ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Instala pacotes Python dentro do venv
COPY requirements.txt /tmp/requirements.txt
RUN pip install --upgrade pip && pip install --no-cache-dir -r /tmp/requirements.txt

# Instala pacotes R
RUN Rscript -e "install.packages(c('forecast','openxlsx','season','MASS','ggplot2','stats','gdata','survival','lubridate','robustbase','matrixStats','xlsx'), repos='http://cran.r-project.org')"

WORKDIR /app
COPY . /app

EXPOSE 8000

CMD ["python", "manage.py", "runserver", "0.0.0.0:8000"]
