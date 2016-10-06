# VERSION 1.7.1.3-2
# AUTHOR: Maksim Pecherskiy
# DESCRIPTION: Basic Airflow container, Forked from puckel/docker-airflow
# BUILD: docker build --rm -t mrmaksimize/airflow
# SOURCE: https://github.com/mrmaksimize/airflow

FROM debian:jessie
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

RUN set -ex \
    && buildDeps=' \
        python-pip \
        python-dev \
        libkrb5-dev \
        libsasl2-dev \
        libssl-dev \
        libffi-dev \
        build-essential \
        libblas-dev \
        liblapack-dev \
    ' \
    && echo "deb http://http.debian.net/debian jessie-backports main" >/etc/apt/sources.list.d/backports.list \
    && apt-get clean -yqq \
    && apt-get update -yqq \
    && apt-get install -yqq --no-install-recommends \
        $buildDeps \
        apt-utils \
        curl \
        netcat \
        locales \
    && apt-get install -yqq -t jessie-backports python-requests libpq-dev \
    && apt-get install -yqq --no-install-recommends \
        r-base \
        r-recommended \
        littler \
    && echo 'options(repos = c(CRAN = "https://cran.rstudio.com/"), download.file.method = "libcurl")' >> /etc/R/Rprofile.site \
    && echo 'source("/etc/R/Rprofile.site")' >> /etc/littler.r \
    && ln -s /usr/share/doc/littler/examples/install.r /usr/local/bin/install.r \
    && ln -s /usr/share/doc/littler/examples/install2.r /usr/local/bin/install2.r \
    && ln -s /usr/share/doc/littler/examples/installGithub.r /usr/local/bin/installGithub.r \
    && ln -s /usr/share/doc/littler/examples/testInstalled.r /usr/local/bin/testInstalled.r \
    && install.r docopt \
    && sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 \
    && useradd -ms /bin/bash -d ${AIRFLOW_HOME} airflow \
    && pip install pytz==2015.7 \
    && pip install cryptography \
    && pip install pyOpenSSL \
    && pip install ndg-httpsclient \
    && pip install pyasn1 \
    && pip install psycopg2 \
    && pip install airflow[celery,postgresql,hive,slack,s3]==$AIRFLOW_VERSION \
    && apt-get remove --purge -yqq $buildDeps libpq-dev \
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

RUN chown -R airflow: ${AIRFLOW_HOME} \
    && chmod +x ${AIRFLOW_HOME}/entrypoint.sh

EXPOSE 8080 5555 8793

USER airflow
WORKDIR ${AIRFLOW_HOME}
ENTRYPOINT ["./entrypoint.sh"]