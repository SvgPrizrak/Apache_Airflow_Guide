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
* `docker --version` (проверка версии Docker);
* `docker-compose --version` (проверка версии Docker Compose);
* `Remove-item alias:curl` (такая проблема может встречаться на VSCode на платформе Windows, ***на Linux-платформах вводить скорее всего не надо***);
* `curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.9.0/docker-compose.yaml'` (это команда для получения yaml-файла, версия Apache Airflow может отличаться - см. команду [здесь](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)).
* далее ОБЯЗАТЕЛЬНО создаем в корневой директории файл `.env` и прописываем там следующие параметры:
```docker
AIRFLOW_UID=50000
AIRFLOW_GID=0
```

## 3. Корректировка yaml-файла и тестовый запуск Apache Airflow (первичная проверка, что все работает)
* откорректируем `docker-compose.yaml` для `postgres` (скорее всего просто придется добавить `ports`) и `clickhouse` (полностью добавить все строки, в дефолтной конфигурации clickhouse отсутствует - `CLICKHOUSE_USER` и `CLICKHOUSE_PASSWORD` можно поменять под себя, образ именно `yandex`, на `clickhouse` возникают проблемы с записью файлов):

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
services:
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

* создадим папку `clickhouse_data` в корневой директории (удобнее всего сделать, находясь в корневой директории командой `mkdir clickhouse_data`);
  
* в терминале IDE прописываем:
  ```docker
  docker-compose up airflow-init
  ```
  
- в течение нескольких минут качаются образы и собирается контейнер, далее прописываем команду:
  ```docker
  docker-compose up
  ```
- после инициализации в папку `clickhouse_data` должны добавиться новые папки и файлы;
- заходим на [локалхост](http://localhost:8080/) и вводим данные для входа логин/пароль (по умолчанию `airflow` для логина и пароля) Airflow.

<p align="center">
  <img width="600" height="250" src="https://github.com/SvgPrizrak/Apache_Airflow_Guide/blob/main/pictures/AirFlow_main_menu.png">
</p>

## 4. Добавление новых Python-пакетов
Поскольку установка новых Python-пакетов для Docker-контейнера проходит немного не так как в Jupyter Notebook, то стоит создать 2 файла в корневой директории: `requirements.txt` и `Dockerfile`.
Содержимое файла `requirements.txt` - пакеты для подключения к ClickHouse (актуальные версии `clickhouse-connect` и `clickhouse-driver` см. [здесь](https://pypi.org/project/clickhouse-driver/) и [здесь](https://pypi.org/project/clickhouse-connect/); последний пакет - это пакет, дающий возможность Apache Airflow создавать подключение к ClickHouse - [здесь](https://pypi.org/project/airflow-providers-clickhouse/).
```python
clickhouse-connect==0.7.8
clickhouse-driver==0.2.7
airflow-providers-clickhouse==0.0.1
```

Содержимое файла `Dockerfile` - код, позволяющий устанавливать пакеты через `pip install` (опять-таки внимательно смотрим на версию вашего Apache Airflow):
```docker
FROM apache/airflow:2.9.0
COPY requirements.txt /requirements.txt
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r /requirements.txt
```

После чего следует сделать следующую последовательность действий, запускающую установку пакетов и пересобирающую Docker-контейнер:
* запустить команду в терминале IDE `docker build . --tag extending_airflow:latest`;
* в docker-compose.yaml поменять 52 строку на `image: ${AIRFLOW_IMAGE_NAME:-extending_airflow:latest}` (номер строки может отличаться, важно, что это самый первый `image` в файле);
* запустить команду в терминале IDE `docker compose up -d --no-deps --build airflow-webserver airflow-scheduler`;
* пересобрать контейнер `docker-compose up -d` (если контейнеры были погашены).

P.S. Если конфигурация Python-пакетов будет меняться, то все эти 4 команды надо запускать заново!!!

## 5. Добавление Python-пакета, содержащего ClickHouseOperator
По умолчанию в Apache Airflow отсутствует возможность создавать ClickHouseOperator для создания, изменения и удаления таблиц (и, что самое главное, обновления данных в автоматическом режиме). Этот пакет не удалось поставить через средства из п.4, поэтому пришлось скачать файл из этой [директории](https://github.com/bryzgaloff/airflow-clickhouse-plugin) - надо содержимое папки `airflow_clickhouse_plugin` перенести в папку `dags` корневой директории, чего должно быть достаточно для установки плагина (именно так, чтобы не пришлось править пути). 

В VSCode расположение файлов выглядит следующим образом:
<p align="center">
  <img width="350" height="490" src="https://github.com/SvgPrizrak/Apache_Airflow_Guide/blob/main/pictures/AirFlow_files.png">
</p>

## 6. Создание подключений в DBeaver
Поскольку мы запустили сразу 2 системы, то можем создать 2 соединения: для PostgreSQL и для ClickHouse (в DBeaver: `Базы данных -> Новое соединение`). При создании не забываем поставить галочку в поле `Показать все базы данных` (иначе мы не увидим новые созданные БД в списке). Для PostgreSQL также была создана дополнительная БД - `postgres_test` для наглядности (в ClickHouse БД названа `default` по умолчанию, будем работать с ней).

### 6.1. Параметры подключения для PostgreSQL в DBeaver видны на скришнотах
<p align="center">
  <img width="420" height="490" src="https://github.com/SvgPrizrak/Apache_Airflow_Guide/blob/main/pictures/PostgreSQL_creation_db.png">
</p>

<p align="center">
  <img width="370" height="490" src="https://github.com/SvgPrizrak/Apache_Airflow_Guide/blob/main/pictures/PostgreSQL_creation_new_db.png">
</p>

Единственное, что может меняться в данном подключении - это логин (aka пользователь) и пароль, но тогда следует менять их и в `docker-compose.yaml` (см. п.3 в добавленном коде), по умолчанию они `airflow`, что и написано при настройке подключения.

### 6.2. Параметры подключения для ClickHouse в DBeaver видны на скришноте
<p align="center">
  <img width="460" height="490" src="https://github.com/SvgPrizrak/Apache_Airflow_Guide/blob/main/pictures/ClickHouse_creation_db.png">
</p>

Единственное, что может меняться в данном подключении - это логин (aka пользователь) и пароль, но тогда следует менять их и в `docker-compose.yaml` (см. п.3 в добавленном коде), у меня они `clickhouse_user` и `clickhouse_password`, что и написано при настройке подключения.

### 6.3. Внешний вид в DBeaver
Расположение БД выглядит после настройки подключений следующим образом - можно переходить к следующим пунктам.
<p align="center">
  <img width="780" height="490" src="https://github.com/SvgPrizrak/Apache_Airflow_Guide/blob/main/pictures/databases_configuration.png">
</p>

## 7. Создание подключений в Apache Airflow
Поскольку для создания DAG нам требуется создание подключений не только в DBeaver, но и в Apache Airflow, то следует их создать через `Admin -> Connections`.

Огромное внимание следует обратить на 3 вещи:
* Connection id - то что будет использоваться при создании DAG (в операторах есть параметр postgres_conn_id или clickhouse_conn_id);
* Host - host.docker.internal - прописывается при настройке, все остальное прописывается так же, как и при настройке DBeaver;
* Port для ClickHouse именно 9000, а не 8123;
* Остальное прописывается интуитивно, по аналогии с DBeaver (см. скриншоты).

### 7.1. Параметры подключения для PostgreSQL в Apache Airflow видны на скришнотах
<p align="center">
  <img width="1000" height="490" src="https://github.com/SvgPrizrak/Apache_Airflow_Guide/blob/main/pictures/Postgres_Airflow.png">
</p>

Единственное, что может меняться в данном подключении - это логин (aka пользователь) и пароль, но тогда следует менять их и в `docker-compose.yaml` (см. п.3 в добавленном коде), по умолчанию они `airflow`, что и написано при настройке подключения.

### 7.2. Параметры подключения для ClickHouse в Apache Airflow видны на скришноте
<p align="center">
  <img width="1000" height="490" src="https://github.com/SvgPrizrak/Apache_Airflow_Guide/blob/main/pictures/ClickHouse_Airflow.png">
</p>

Единственное, что может меняться в данном подключении - это логин (aka пользователь) и пароль, но тогда следует менять их и в `docker-compose.yaml` (см. п.3 в добавленном коде), у меня они `clickhouse_user` и `clickhouse_password`, что и написано при настройке подключения.

### 7.3. Внешний вид в Apache Airflow
Расположение БД выглядит после настройки подключений следующим образом - можно переходить к следующим пунктам.
<p align="left">
  <img width="1000" height="250" src="https://github.com/SvgPrizrak/Apache_Airflow_Guide/blob/main/pictures/Airflow_connections.png">
</p>

## 8. Создание DAG и тестирование в Apache Airflow
В папке `dags` корневой директории я создал 2 тестовых файла: `test_postgres_operator_dag.py` и `test_clickhouse_operator_dag.py` - оба файла направлены на добавление таблиц и изменение данных в них, т.е. мы можем проверить правильность выполнения задач как через Apache Airflow, так и через DBeaver.

### 8.1. Содержимое `test_postgres_operator_dag.py`:
```python
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['testmail@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    dag_id='test_postgres_dag',
    start_date=datetime(2024, 2, 2),
    schedule="@once",
    catchup=False,
) as dag:
    create_pet_table = PostgresOperator(
        task_id="create_pet_table",
        postgres_conn_id="postgres_local_test",
        sql="""
            CREATE TABLE IF NOT EXISTS pet (
            pet_id INT NOT NULL PRIMARY KEY,
            name VARCHAR NOT NULL,
            pet_type VARCHAR NOT NULL,
            birth_date DATE NOT NULL,
            owner VARCHAR NOT NULL);
          """
    )

    insert_to_pet_table = PostgresOperator(
        task_id="insert_to_pet_table",
        postgres_conn_id="postgres_local_test",
        sql="""
            INSERT INTO pet (pet_id, name, pet_type, birth_date, owner)
            VALUES (1, 'Max', 'Dog', '2018-07-05', 'Jane');
            INSERT INTO pet (pet_id, name, pet_type, birth_date, owner)
            VALUES (2, 'Susie', 'Cat', '2019-05-01', 'Phil');
            INSERT INTO pet (pet_id, name, pet_type, birth_date, owner)
            VALUES (3, 'Lester', 'Hamster', '2020-06-23', 'Lily');
            INSERT INTO pet (pet_id, name, pet_type, birth_date, owner)
            VALUES (4, 'Quincy', 'Parrot', '2013-08-11', 'Anne');
            """
    )

    delete_from_pet_table = PostgresOperator(
        task_id="delete_from_pet_table",
        postgres_conn_id="postgres_local_test",
        sql="""
            DELETE FROM pet WHERE pet_id <= 4
            """
    )

    create_pet_table >> delete_from_pet_table >> insert_to_pet_table
```

### 8.2. Содержимое `test_clickhouse_operator_dag.py`:
```python
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['testmail@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    dag_id='test_clickhouse_dag',
    start_date=datetime(2024, 2, 2),
    schedule="@once",
    catchup=False,
) as dag:
    create_pet_table = ClickHouseOperator(
        task_id="create_pet_table",
        database='default',
        clickhouse_conn_id="clickhouse_local_test",
        sql="""
            CREATE TABLE IF NOT EXISTS employee
            (emp_no  UInt32 NOT NULL)
            ENGINE = MergeTree()
            PRIMARY KEY (emp_no);
          """
    )

    create_pet_table
```

### 8.3. Внешний вид DAG (обновления тасок) в Apache Airflow (добавил еще несколько тестовых для проверки BashOperator и PythonOperator) и новых таблиц в DBeaver
<p align="left">
  <img width="1000" height="170" src="https://github.com/SvgPrizrak/Apache_Airflow_Guide/blob/main/pictures/Airflow_DAGs.PNG">
</p>

<p align="center">
  <img width="500" height="500" src="https://github.com/SvgPrizrak/Apache_Airflow_Guide/blob/main/pictures/DBeaver_tables.png">
</p>

P.S. Подключаться к БД ClickHouse теперь возможно 2 способами: через `airflow_clickhouse_plugin` и через `clickhouse_connect` (см. п.9 - тестовые DAG были созданы именно ради этого).
## 9. Файлы
Копия корневой директории `airflow_docker` есть в списке файлов данного репозитория с уже исправленными/добавленными файлами и библиотеками, а также всеми тестовыми DAG. После скачивания и распаковки архива `airflow_docker.rar` нужно проделать следующие действия:
* проверить наличие всех программ из п.1 и вводных данных;
* выбрать в IDE директорию `airflow_docker` в качестве той, в которой будем вводить команды;
* запустить команды в терминале IDE `docker build . --tag extending_airflow:latest`, `docker compose up -d --no-deps --build airflow-webserver airflow-scheduler`, `docker-compose up -d` (должны установиться файлы в `clickhouse_data` и библиотеки, кроме ClickHouseOperator - она уже находится в корневой директории);
* перейти к п.6-8 и начать создавать подключения в DBeaver и Apache Airflow.
