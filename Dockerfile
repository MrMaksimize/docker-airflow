# VERSION 1.10.1
# AUTHOR: Maksim Pecherskiy
# DESCRIPTION: Airflow container for running City of San Diego Airflow Instances.  Original work by Puckel_
# BUILD: docker build --rm -t mrmaksimize/docker-airflow .
# SOURCE: https://github.com/mrmaksimize/docker-airflow

FROM python:3.6
LABEL maintainer="mrmaksimize"


# Never prompts the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

# Airflow
ARG AIRFLOW_VERSION=1.10.1
ARG AIRFLOW_HOME=/usr/local/airflow
ARG AIRFLOW_DEPS=""
ARG PYTHON_DEPS=""
ENV AIRFLOW_GPL_UNIDECODE yes

# Define en_US.
ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LC_MESSAGES en_US.UTF-8


# Oracle Essentials
ENV ORACLE_HOME /opt/oracle
ENV ARCH x86_64
ENV DYLD_LIBRARY_PATH /opt/oracle
ENV LD_LIBRARY_PATH /opt/oracle

# Set Install Lists
RUN set -ex \
    && buildDeps=' \
        freetds-dev \
        libkrb5-dev \
        libsasl2-dev \
        libssl-dev \
        libffi-dev \
        libpq-dev \
        git \
    ' \
    && additions='
        #python-dev \
        #build-essential \
        #libcurl4-gnutls-dev \
        #libnetcdf-dev \
        #libpoppler-dev \
        #libhdf4-alt-dev \
        #libhdf5-serial-dev \
        #libblas-dev \
        #liblapack-dev \
        #libpq-dev \
        #libgdal-dev \
        #libproj-dev \
        #libgeos-dev \
        #libspatialite-dev \
        #libspatialindex-dev \
        #libfreetype6-dev \
        #libxml2-dev \
        #libxslt-dev \
        #gnupg2 \
        #libsqlite3-dev \
        #zlib1g-dev \
        #python-pip \
        curl \
        #netcat \
        #cython \
        #python-numpy \
        #python-gdal \
        #libaio1 \
        unzip \
        less \
        #freetds-dev \
        smbclient \
        vim \
        wget \
        #gdal-bin \
        #sqlite3 \
        #python-requests \
        #libpq-dev \
    '

# Update apt and install
RUN apt-get update -yqq \
    && apt-get upgrade -yqq \
    && apt-get install -yqq --no-install-recommends \
        $buildDeps \
        $additions \
        freetds-bin \
        build-essential \
        default-libmysqlclient-dev \
        apt-utils \
        curl \
        rsync \
        netcat \
        locales

# Update Locales, add Airflow User
RUN sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 \
    && useradd -ms /bin/bash -d ${AIRFLOW_HOME} airflow

# NodeJS packages
RUN curl -sL https://deb.nodesource.com/setup_7.x | bash - \
    && apt-get install -y nodejs \
    #&& npm install -g mapshaper \
    #&& npm install -g turf-cli \
    #&& npm install -g geobuf \
    #&& npm install -g @mapbox/mapbox-tile-copy

RUN pip install -U pip setuptools wheel \
    && pip install pytz \
    && pip install pyOpenSSL \
    && pip install ndg-httpsclient \
    && pip install pyasn1 \
    ## Additions
    #&& pip install Cython \
    #&& pip install packaging \
    #&& pip install appdirs \
    ##&& pip install pytz==2015.7 \
    #&& pip install psycopg2 \
    #&& pip install mysql-python \
    #&& pip install requests \
    #&& pip install logging \
    #&& pip install boto3 \
    #&& pip install geojson \
    #&& pip install httplib2 \
    #&& pip install pymssql \
    && pip install pandas \
    && pip install xlrd \
    #&& pip install autodoc==0.3 \
    #&& pip install Sphinx==1.5.1 \
    #&& pip install celery==4.0.2 \
    #&& pip install beautifulsoup4==4.5.3 \
    && pip install lxml==3.7.3 \
    #&& pip install ipython==5.3.0 \
    #&& pip install jupyter \
    #&& pip install password \
    #&& pip install Flask-Bcrypt \
    #&& pip install geomet==0.1.1 \
    #&& pip install geopy==1.11 \
    #&& pip install rtree \
    #&& pip install shapely \
    #&& pip install fiona \
    #&& pip install descartes \
    #&& pip install pyproj \
    && pip install geopandas \
    #&& pip install requests==2.13.0 \
    && pip install PyGithub \
    && pip install keen \
    #&& pip install apache-airflow[crypto,celery,postgres,hive,slack,s3,jdbc,mysql,ssh${AIRFLOW_DEPS:+,}${AIRFLOW_DEPS}]==${AIRFLOW_VERSION} \
    && pip install apache-airflow[crypto,celery,postgres,slack,s3,jdbc,mysql,mssql,ssh,password,rabbitmq,samba,redis]==${AIRFLOW_VERSION} \
    && pip install 'redis>=2.10.5,<3' \
    #&& if [ -n "${PYTHON_DEPS}" ]; then pip install ${PYTHON_DEPS}; fi \
    && apt-get purge --auto-remove -yqq $buildDeps \
    && apt-get purge --auto-remove -yqq $additions \
    && apt-get autoremove -yqq --purge \
    #&& apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base

COPY script/entrypoint.sh /entrypoint.sh
COPY config/airflow.cfg ${AIRFLOW_HOME}/airflow.cfg

RUN chown -R airflow: ${AIRFLOW_HOME}

EXPOSE 8080 5555 8793

USER airflow
WORKDIR ${AIRFLOW_HOME}
ENTRYPOINT ["/entrypoint.sh"]
CMD ["webserver"] # set default arg for entrypoint

