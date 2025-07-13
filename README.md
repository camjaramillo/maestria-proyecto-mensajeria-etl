# Maestría en Analítica e Inteligencia de Negocios
## Curso Introducción a la Ingeniería de Datos y Big Data

### Proyecto Mensajería - ETL

## 🐍 Configuración del Entorno Virtual
Para garantizar que las dependencias de este proyecto no interfieran con otras instalaciones de Python en tu sistema, es recomendable crear un entorno virtual. Sigue estos pasos según tu sistema operativo:

<br>1. Crear el Entorno Virtual</br>

En Windows:

python -m venv .venv

En macOS o Linux:

python3 -m venv .venv

Esto creará un directorio oculto .venv en el que se instalarán las dependencias del proyecto.

<br>2. Activar el Entorno Virtual</br>

En Windows:

.venv\Scripts\activate

En macOS o Linux:

source .venv/bin/activate

Una vez activado, el prompt de tu terminal debería mostrar el nombre del entorno entre paréntesis, indicando que está activo.

<br>3. Instalar las Dependencias</br>

Con el entorno virtual activado, instala las bibliotecas necesarias para el proyecto ejecutando:

python.exe -m pip install --upgrade pip
pip install -r requirements.txt

Esto instalará todas las dependencias listadas en el archivo requirements.txt, asegurando que el entorno esté configurado correctamente.

<br>4. Configuración del archivo config.yaml</br>

Ruta: /config/config.yaml

Para establecer las conexiones con las bases de datos de origen y destino, es necesario configurar el archivo config.yaml, con base en las configuraciones de tu postgreSQL.

Ten en cuenta que el ambiente <i>'postgres_db_src'</i> hace referencia a la base de datos fuente, mientras que <i>'postgres_db_tgt'</i>  indica la base de datos destino (Data Warehouse). Asegúrate de crear ambas bases de datos, pueden tener el nombre que prefieras (luego el nombre será configurado en el parámetro "database").

Parámetros a configurar:

- host: Dirección del servidor de la base de datos. Utiliza localhost si la base de datos está en el mismo servidor que la aplicación.

- port: Puerto de conexión. El valor predeterminado de PostgreSQL es 5432.

- database: Nombre de la base de datos a la que deseas conectarte.

- user: Usuario con permisos adecuados para acceder a la base de datos.

- password: Contraseña asociada al usuario especificado.

- timeout: Tiempo máximo (en segundos) que la aplicación esperará para establecer una conexión antes de generar un error.

- pool_size: Número máximo de conexiones simultáneas que la aplicación puede mantener con la base de datos.

Asegúrate de ajustar estos valores según tu entorno específico.

<br>5. Ejecutar el ETL</br>

Para ejecutar el ETL completo, se debe escribir el comando:

python main.py

<br>6. Desactivar el Entorno Virtual (opcional)</br>

Si necesitas desactivar el entorno, en la términal debes escribir:

deactivate

<hr>

## Despliegue del proyecto con Docker

<br>1. Ejecutar el Dockerfile</br>

docker build -t mensajeria-python-image .

Al hacer esto, se creara la imagen mensajeria-python-image en Docker.


<br>2. Iniciar la base de datos de Airflow (una sola vez)</br>

docker compose up airflow-init

<br>3. Iniciar todos los contenedores para ejecutar Airflow</br>

docker-compose up


<br>(opc) Iniciar los servicios principales de AirFlow</br>

docker compose up -d airflow-webserver airflow-scheduler

<br>4. Detener todos los contenedores de Airflow</br>

Para detener el despliegue del proyecto, ejecutar:

docker-compose stop

O para eliminar los contenedores

docker-composer down