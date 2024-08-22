# imports
import boto3
import pandas as pd
import os
from dotenv import load_dotenv
from io import StringIO
import datetime

# .env
load_dotenv()

region_name = os.getenv('AWS_REGION')
aws_access_key_id = os.getenv('AWS_ACCESS_KEY')
aws_secret_access_key = os.getenv('AWS_SECRET_KEY')
bucket_name = os.getenv('AWS_BUCKET_STORES_RAW')
bucket_bronze_name = os.getenv('AWS_BUCKETS_STORES_STAGING')

s3 = boto3.resource(
  service_name='s3',
  region_name=region_name,
  aws_access_key_id = aws_access_key_id,
  aws_secret_access_key = aws_secret_access_key,
  )


def load_csvs_from_s3_to_dataframe(bucket_name, s3_folder):
    # Criar sessão no S3
    s3_client = s3.meta.client

    # Listar todos os arquivos .csv na pasta stores-raw/
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_folder)

    # Inicializar uma lista para armazenar os DataFrames
    dataframes = []

    for obj in response.get('Contents', []):
        # Verifica se o arquivo é um .csv e contem stores
        if obj['Key'].endswith('.csv') and 'stores' in obj['Key']:
            # Pega o objeto do S3
            csv_obj = s3_client.get_object(Bucket=bucket_name, Key=obj['Key'])

            # Lê o conteúdo do objeto como um DataFrame
            csv_data = csv_obj['Body'].read().decode('utf-8')
            df = pd.read_csv(StringIO(csv_data))

            # Adiciona o DataFrame à lista
            dataframes.append(df)

    # Combina todos os DataFrames em um único
    if dataframes:
        combined_df = pd.concat(dataframes, ignore_index=True)
    else:
        combined_df = pd.DataFrame()  # Retorna um DataFrame vazio se nenhum CSV for encontrado

    return combined_df

# Exemplo de uso
bucket_name = 'sales-store-datalake-raw'
s3_folder = 'stores-raw/'  # Pasta dentro do bucket

# Chama a função e salva o DataFrame
df_combined = load_csvs_from_s3_to_dataframe(bucket_name, s3_folder)

# Transforma a coluna ´´Date´´ para pd.datetime
df_combined['Date'] = pd.to_datetime(df_combined['Date'])

# Extração do ano da coluna ´´Date´´
df_combined['Year'] = df_combined['Date'].dt.year

# Filtrar o dataframe para retornar apenas o último ano
df_latest_year = df_combined[df_combined['Year'] == df_combined['Year'].max()]

# Criar um objeto s3 com o DataFrame filtrado
now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
s3.meta.client.put_object(Body=df_latest_year.to_csv(index=False), Bucket=bucket_bronze_name, Key=f'stores-bronze/stores_bronze.csv')
