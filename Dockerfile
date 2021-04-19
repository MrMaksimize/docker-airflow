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
ARG AIRFLOW_VERSION=2.0.0
ARG AIRFLOW_HOME=/usr/local/airflow
ARG GDAL_VERSION=3.2.2
# Not currently using this because of snowflake
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.7.txt"

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
        unixodbc \
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
    && npm install -g mapshaper

# populate "ocbcinst.ini" as this is where ODBC driver config sits
#RUN echo "[FreeTDS]\n\
    #Description = FreeTDS Driver\n\
    #Driver = /usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so\n\
    #Setup = /usr/lib/x86_64-linux-gnu/odbc/libtdsS.so" >> /etc/odbcinst.ini

RUN pip install -U pip==20.2.4 setuptools wheel \
    && pip install apache-airflow[amazon,celery,jdbc,mysql,microsoft.azure,microsoft.mssql,odbc,oracle,postgres,snowflake,ssh,redis,rabbitmq,samba]==${AIRFLOW_VERSION} \
    apache-airflow-providers-amazon \
    arcgis \
    boto3 \
    fiona \
    #GDAL==$(gdal-config --version | awk -F'[.]' '{print $1"."$2}') --global-option=build_ext --global-option="-I/usr/include/gdal" \
    geobuf \
    geojson \
    geopandas \
    google-api-python-client \
    oauth2client \
    openpyxl \
    pyodbc \
    pandas \
    PyGithub \
    requests \
    rtree \
    s3fs \
    shapely \
    xlrd \
    --constraint "${CONSTRAINT_URL}"

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

RUN apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base

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

