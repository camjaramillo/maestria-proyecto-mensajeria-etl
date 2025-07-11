FROM python:3.12-slim

# Crear y establecer el directorio de trabajo
WORKDIR /app

# Copiar los archivos necesarios
COPY . /app

# Instalar dependencias
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Ejecutar el ETL (puedes cambiar esto por tu main real)
CMD ["python", "main.py"]
