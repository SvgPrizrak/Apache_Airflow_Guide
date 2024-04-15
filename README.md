# Apache Airflow Docker + PostgreSQL/ClickHouse
Этот гайд предназначен для тех, кто хочет развернуть Apache Airflow через Docker, а также получить доступ к PostgreSQL и ClickHouse. Все действия выполнялись на ОС Windows 10/11, в остальных ОС принцип установки абсолютно аналогичен, часть команд могут выполняться через командную строку, а не графический интерфейс.
Необходимый стек для выполнения процедур: 
* DBeaver ([скачать](https://dbeaver.io/), или иная СУБД, способная подключаться к PostgreSQL и ClickHouse);
* VSCode ([скачать](https://code.visualstudio.com/), я использовал его) / PyCharm ([скачать](https://www.jetbrains.com/ru-ru/pycharm/)) или любая другая IDE, работающая с Python;
* Python ([скачать](https://www.python.org/), можно взять самую новую версию проблем быть не должно - на момент написания гайда 3.12.2);
* Docker (см. ниже).

## 1. Установка Docker и Docker Compose
Все необходимое есть на сайте - они ставятся сразу вместе: [скачать](https://www.docker.com/products/docker-desktop/).

## 2. Создаем папку (на моем примере это будет папка airflow_docker - далее, корневая директория)
В созданной папке создаем еще 3 папки: `logs`, `dags`, `plugins`, после чего зайти в IDE в корневую директорию и ввести следующие команды:
* Remove-item alias:curl (такая проблема может встречаться на VSCode на платформе Windows);
* curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.9.0/docker-compose.yaml' (это команда для получения yaml-файла, версия Apache Airflow может отличаться - см. команду [здесь](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)).
* далее ОБЯЗАТЕЛЬНО создаем в корневой директории файл '.env' и прописываем там следующие параметры:
<p align="center">
  <img width="300" height="80" src="https://raw.githubusercontent.com/SvgPrizrak/Apache_Airflow_Guide/main/pictures/AirFlow_Users.png">
</p>
* откорректируем docker-compose.yaml для `postgres` и `clickhouse`:
services:
  postgres:
    image: postgres:13
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - postgres-db-volume:/var/lib/postgresql/data
    ports:
      - 5432:5432
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 10s
      retries: 5
      start_period: 5s
    restart: always

