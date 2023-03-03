FROM ubuntu:latest

LABEL maintainer="projet_MOOC"

# Installation de MongoDB
RUN apt-get update && apt-get install -y mongodb

# Installation de MySQL
RUN apt-get update && \
    apt-get install -y mysql-server && \
    /etc/init.d/mysql start && \
    mysql -u root -e "CREATE DATABASE mydb"

# Installation de Lazydocker
RUN apt-get update && \
    apt-get install -y git && \
    git clone https://github.com/jesseduffield/lazydocker.git && \
    mv lazydocker /usr/local/bin && \
    chmod +x /usr/local/bin/lazydocker

# Exposition des ports
EXPOSE 27017
EXPOSE 3306

# Commande pour lancer MongoDB, MySQL et Lazydocker
CMD mongod & service mysql start && lazydocker
