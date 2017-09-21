# VERSION 1.7.1.3-5
# AUTHOR: Maksim Pecherskiy
# DESCRIPTION: Basic Airflow container, Forked from puckel/docker-airflow
# BUILD: docker build --rm -t mrmaksimize/airflow .
# SOURCE: https://github.com/mrmaksimize/airflow

FROM debian:latest
MAINTAINER mrmaksimize

# Never prompts the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

# Airflow
ARG AIRFLOW_VERSION=1.7.1.3
ENV AIRFLOW_HOME /usr/local/airflow

# Define en_US.
ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LC_MESSAGES en_US.UTF-8
ENV LC_ALL  en_US.UTF-8
ENV LC_ALL=C

# GDAL data env
ENV GDAL_DATA /usr/share/gdal/2.1

# Oracle Essentials
ENV ORACLE_HOME /opt/oracle
ENV ARCH x86_64
ENV DYLD_LIBRARY_PATH /opt/oracle
ENV LD_LIBRARY_PATH /opt/oracle

# Debian packages and libraries
RUN set -ex \
    && buildDeps=' \
        python-dev \
        libkrb5-dev \
        libsasl2-dev \
        libssl-dev \
        libffi-dev \
        build-essential \
        libcurl4-gnutls-dev \
        libnetcdf-dev \
        libpoppler-dev \
        libhdf4-alt-dev \
        libhdf5-serial-dev \
        libblas-dev \
        liblapack-dev \
        libpq-dev \
        libgdal-dev \
        libproj-dev \
        libgeos-dev \
        libspatialite-dev \
        libspatialindex-dev \
        libfreetype6-dev \
        libxml2-dev \
        libxslt-dev \
        gnupg2 \
        libsqlite3-dev \
        zlib1g-dev \
    ' \
    && echo "deb http://http.debian.net/debian jessie-backports main" >/etc/apt/sources.list.d/backports.list \
    && apt-get clean -yqq \
    && apt-get update -yqq \
    && apt-get install -yqq --no-install-recommends \
        $buildDeps \
        python-pip \
        apt-utils \
        curl \
        git \
        netcat \
        locales \
        mdbtools \
        cython \
        python-numpy \
        python-gdal \
        libaio1 \
        unzip \
        less \
        freetds-dev \
        smbclient \
        vim \
        wget \
        gdal-bin \
        sqlite3 \
    && apt-get install -yqq -t jessie-backports python-requests libpq-dev

#Locales
RUN sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 \
    && useradd -ms /bin/bash -d ${AIRFLOW_HOME} airflow

# Tippecanoe
WORKDIR /tmp
RUN git clone https://github.com/mapbox/tippecanoe.git \
    && cd ./tippecanoe \
    && make \
    && make install
WORKDIR {AIRFLOW_HOME}

# NodeJS packages
RUN curl -sL https://deb.nodesource.com/setup_7.x | bash - \
    && apt-get install -y nodejs \
    && npm install -g mapshaper \
    && npm install -g turf-cli \
    && npm install -g geobuf \
    && npm install -g @mapbox/mapbox-tile-copy

# Python packages
RUN pip install -U pip \
    && pip install Cython \
    && pip install packaging \
    && pip install appdirs \
    && pip install pytz==2015.7 \
    && pip install pyOpenSSL \
    && pip install ndg-httpsclient \
    && pip install pyasn1 \
    && pip install psycopg2 \
    && pip install requests \
    && pip install logging \
    && pip install boto3 \
    && pip install geojson \
    && pip install httplib2 \
    && pip install pymssql \
    && pip install pandas==0.19.2 \
    && pip install xlrd==1.0.0 \
    && pip install autodoc==0.3 \
    && pip install Sphinx==1.5.1 \
    && pip install celery==4.0.2 \
    && pip install beautifulsoup4==4.5.3 \
    && pip install lxml==3.7.3 \
    && pip install ipython==5.3.0 \
    && pip install jupyter \
    && pip install password \
    && pip install Flask-Bcrypt \
    && pip install geomet==0.1.1 \
    && pip install geopy==1.11 \
    && pip install rtree \
    && pip install shapely \
    && pip install fiona \
    && pip install descartes \
    && pip install pyproj \
    && pip install geopandas \
    && pip install requests==2.13.0 \
    && pip install PyGithub==1.32 \
    && pip install keen==0.3.31 \
    && pip install airflow[celery,postgres,hive,slack,jdbc,s3,crypto,jdbc]==$AIRFLOW_VERSION

# Cleaning & setup
RUN apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base

COPY script/entrypoint.sh ${AIRFLOW_HOME}/entrypoint.sh
COPY config/airflow.cfg ${AIRFLOW_HOME}/airflow.cfg
COPY assets/oracle.zip  ${AIRFLOW_HOME}/oracle.zip

RUN unzip ${AIRFLOW_HOME}/oracle.zip -d /opt \
&& env ARCHFLAGS="-arch $ARCH" pip install cx_Oracle \
&& rm ${AIRFLOW_HOME}/oracle.zip

RUN chown -R airflow: ${AIRFLOW_HOME} \
    && chmod +x ${AIRFLOW_HOME}/entrypoint.sh \
    && chown -R airflow /usr/lib/python* /usr/local/lib/python* \
    && chown -R airflow /usr/lib/python2.7/* /usr/local/lib/python2.7/* \
    && chown -R airflow /usr/local/bin* /usr/local/bin/* \
    && sed -i "s|flask.ext.cache|flask_cache|g" /usr/local/lib/python2.7/dist-packages/flask_cache/jinja2ext.py

EXPOSE 8080 5555 8793

USER airflow
WORKDIR ${AIRFLOW_HOME}
ENTRYPOINT ["./entrypoint.sh"]
