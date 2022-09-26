FROM apache/airflow:2.2.2-python3.9

USER root
# Installation of orracle os packages
RUN apt-get update && apt-get install -y libaio1 wget unzip
WORKDIR /opt/oracle
RUN wget https://download.oracle.com/otn_software/linux/instantclient/instantclient-basiclite-linuxx64.zip && \
    unzip instantclient-basiclite-linuxx64.zip && rm -f instantclient-basiclite-linuxx64.zip && \
    cd /opt/oracle/instantclient* && rm -f *jdbc* *occi* *mysql* *README *jar uidrvci genezi adrci && \
    echo /opt/oracle/instantclient* > /etc/ld.so.conf.d/oracle-instantclient.conf && ldconfig

# Installation of mysql client os package
RUN apt-get install mysql-client -y  # -y to say yes to Y/N prompts

USER airflow
RUN pip install srsly==2.4.3 pymongo==4.1.1 #pip install bson did not work
RUN pip install apache-airflow-providers-oracle
RUN pip install cx_Oracle
RUN pip install azure-storage-blob==12.7.1
RUN pip install apache-airflow-providers-microsoft-azure==4.0.0
RUN pip install sodapy


WORKDIR /opt/airflow
