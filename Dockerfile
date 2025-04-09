FROM apache/airflow:2.8.4

USER root

ARG AIRFLOW_VERSION=2.8.4
ARG AIRFLOW_HOME=/opt/airflow

RUN apt-get update && \
    apt-get install -y default-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    unzip \
    libgconf-2-4 \
    libnss3 \
    libxss1 \
    libasound2 \
    libxtst6 \
    libgtk-3-0 \
    libgbm1 \
    xvfb \
    fonts-liberation \
    libappindicator3-1 \
    xdg-utils

RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - && \
    echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list && \
    apt-get update && \
    apt-get install -y google-chrome-stable && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN chown -R airflow: ${AIRFLOW_HOME}
USER airflow

COPY ./requirements.txt ${AIRFLOW_HOME}/requirements.txt
RUN pip install -r ${AIRFLOW_HOME}/requirements.txt
RUN pip install chromedriver_autoinstaller

EXPOSE 8080 5555 3306 8812 9000 5432 9009

WORKDIR ${AIRFLOW_HOME}

ENV AIRFLOW_HOME /opt/airflow
