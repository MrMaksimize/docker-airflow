# VERSION 1
# AUTHOR: City of San Diego
# DESCRIPTION: Airflow container for running City of San Diego Airflow Instances.
# BUILD: docker build --rm -t cityofsandiego/airflow:latest .
# SOURCE: https://github.com/DataSD/poseidon-airflow

FROM python:3.7
LABEL maintainer="City of San Diego"

# Never prompts the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

# Airflow
ARG AIRFLOW_VERSION=1.10.11
ARG AIRFLOW_HOME=/usr/local/airflow
ARG GDAL_VERSION=2.1.0

ENV AIRFLOW_GPL_UNIDECODE yes

# Define en_US.
ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LC_MESSAGES en_US.UTF-8

# GDAL ENV
ENV GDAL_DATA /usr/share/gdal
ENV GDAL_VERSION $GDAL_VERSION
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

#R
ENV R_BASE_VERSION 3.5.3

RUN pwd

# Update apt and install
RUN apt-get update -yqq \
    && apt-get upgrade -yqq \
    && apt-get install -yqq --no-install-recommends \
        apt-utils \
        build-essential \
        curl \
        freetds-bin \
        freetds-dev \
	gdal-bin \
        git \
        gnupg2 \
        less \
        locales \
        libaio1 \
        libcurl4-gnutls-dev \
	libgdal20 \
        libgdal-dev \
        libgeos-dev \
        libhdf4-alt-dev \
        libhdf5-serial-dev \
        libnetcdf-dev \
        libpoppler-dev \
        libproj-dev \
        libpq-dev \
        libspatialindex-dev \
        libspatialite-dev \
        libxml2-dev \
        netcat \
        pandoc \
        python3-software-properties \
        python3-dev \
        python3-numpy \
        rsync \
        software-properties-common \
        smbclient \
        sqlite3 \
        unzip \
        vim \
        wget

# Update Locales, add Airflow User
RUN sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 \
    && useradd -ms /bin/bash -d ${AIRFLOW_HOME} airflow


# NodeJS packages
RUN curl -sL https://deb.nodesource.com/setup_10.x | bash - \
    && apt-get install -y nodejs \
    && npm install -g mapshaper \
    && npm install -g geobuf


RUN pip install -U pip setuptools wheel \
    && pip install apache-airflow[crypto,celery,postgres,s3,jdbc,mysql,mssql,oracle,ssh,password,rabbitmq,samba]==${AIRFLOW_VERSION} \
    "arcgis==1.8.1" \
    "boto3==1.12.26" \
    "fiona==1.8.13.post1" \
    "gdal==2.1.0" \
    git+https://github.com/jguthmiller/pygeobuf.git@geobuf-v3 \
    "geojson==2.5.0" \
    "geopandas==0.8.0" \
    "google-api-python-client==1.9.3" \
    "oauth2client==4.1.3" \
    "pandas==1.0.5" \
    "PyGithub==1.51" \
    "redis==3.5.3" \
    "requests>=2.20" \
    "shapely==1.7.0" \
    "snowflake-connector-python==2.2.8" 

RUN apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base

# R Installs

## Install R
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
      build-essential r-base r-cran-hexbin r-cran-plotly

RUN Rscript -e "install.packages(c('data.table', \
    'docopt', \
    'dplyr', \
    'ggplot2', \
    'crosstalk', \
    'DT', \
    'dygraphs', \
    'flexdashboard', \
    'leaflet', \
    'mgcv', \
    'rmarkdown', \
    'rsconnect', \
    'shiny', \
    'tidyr', \
    'viridis' \
))"

RUN chown -R airflow /usr/local/lib/R/site-library* /usr/local/lib/R/site-library/*

COPY script/entrypoint.sh ${AIRFLOW_HOME}/entrypoint.sh
COPY config/airflow.cfg ${AIRFLOW_HOME}/airflow.cfg

WORKDIR /opt/oracle

RUN wget https://download.oracle.com/otn_software/linux/instantclient/instantclient-basiclite-linuxx64.zip && \
    unzip instantclient-basiclite-linuxx64.zip && \
    rm -f instantclient-basiclite-linuxx64.zip && \
    cd instantclient* && \
    rm -f *jdbc* *occi* *mysql* *jar uidrvci genezi adrci && \
    echo /opt/oracle/instantclient* > /etc/ld.so.conf.d/oracle-instantclient.conf && \
    ldconfig

WORKDIR /

RUN chown -R airflow: ${AIRFLOW_HOME} \
    && chmod +x ${AIRFLOW_HOME}/entrypoint.sh \
    && chown -R airflow /usr/lib/python* /usr/local/lib/python* \
    && chown -R airflow /usr/local/bin* /usr/local/bin/*

EXPOSE 8080 5555 8793

USER airflow
WORKDIR ${AIRFLOW_HOME}
ENTRYPOINT ["./entrypoint.sh"]
CMD ["webserver"] # set default arg for entrypoint

