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
В корневой директории создаем еще 3 папки: `logs`, `dags`, `plugins`, после чего зайти через IDE в корневую директорию и ввести следующие команды:
* docker --version (проверка версии Docker);
* docker-compose --version (проверка версии Docker Compose);
* Remove-item alias:curl (такая проблема может встречаться на VSCode на платформе Windows, ***на Linux-платформах вводить скорее всего не надо***);
* curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.9.0/docker-compose.yaml' (это команда для получения yaml-файла, версия Apache Airflow может отличаться - см. команду [здесь](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)).
* далее ОБЯЗАТЕЛЬНО создаем в корневой директории файл `.env` и прописываем там следующие параметры:
<p align="center">
  <img width="300" height="80" src="https://raw.githubusercontent.com/SvgPrizrak/Apache_Airflow_Guide/main/pictures/AirFlow_Users.png">
</p>

## 3. Корректировка yaml-файла и тестовый запуск Apache Airflow (первичная проверка, что все работает)
* откорректируем `docker-compose.yaml` для `postgres` (скорее всего просто придется добавить `ports`) и `clickhouse` (полностью добавить все строки, в дефолтной конфигурации clickhouse отсутствует - `CLICKHOUSE_USER` и `CLICKHOUSE_PASSWORD` можно поменять под себя):

```docker
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
```

```docker
clickhouse:
    image: yandex/clickhouse-server
    restart: always
    ports:
      - "8123:8123"  # Порт для HTTP-интерфейса
      - "9000:9000"  # Порт для внешних подключений
    volumes:
      - ./clickhouse_data:/var/lib/clickhouse
      - ./clickhouse_data:/config:/etc/clickhouse-server
    environment:
      - CLICKHOUSE_CONFIG_DIR=/etc/clickhouse-server
      - CLICKHOUSE_USER=clickhouse_user
      - CLICKHOUSE_PASSWORD=clickhouse_password
      # Другие переменные окружения для настройки ClickHouse
```

* создадим папку `clickhouse_data` в корневой директории (удобнее всего сделать, находясь в корневой директории командой `mkdir clickhouse_data`)
  
* в терминале IDE прописываем:
  ```docker
  docker-compose up airflow-init
  ```
  
- в течение нескольких минут качаются образы и собирается контейнер, далее прописываем команду:
  ```docker
  docker-compose up
  ```
  
- заходим на [локалхост](http://localhost:8080/) и вводим данные для входа логин/пароль (по умолчанию `airflow` для логина и пароля) Airflow.

<p align="center">
  <img width="600" height="220" src="https://github.com/SvgPrizrak/Apache_Airflow_Guide/blob/main/pictures/AirFlow_main_menu.png">
</p>
