#!/usr/bin/env python
# coding: utf-8

###################################################################################
# Este código tem como objetivo gerar um arquivo de definição da tabela que passará 
# pelo quality com intuito de validação dos campos, no ambito de colunas e tipo de 
# dado esperado.
#
#
# TODO: Dito isso, é necessário adaptar o código para cada fonte de dado esperada.
#       No caso deste exemplo será considerada uma base de dados mockada como
#       da informação.
###################################################################################

import os
import pandas as pd
import awswrangler as wr
import great_expectations as ge

from aws.utils.main import get_env

context = 'mysql'
table = 'churn'
system = 'sales'
db = 'customers' # Database name

bucket = f'gcarr-{get_env()}-artifacts'
prefix = f'quality/{system}/{db}/{table}/'


def get_definition(context: str, table: str):
    mapping = {
        'field': [],
        'type': [],
        'nullable': []
    }
    success = False
    if context == 'mysql':
        import pymysql
        
        # TODO: Mudar a chamada de conexão. Inserir secret manager.
        conn = pymysql.connect(
            host='localhost',
            user='dev',
            password='root',
            database='customers'
        )
        cursor = conn.cursor()
        sql = f"""
            SHOW COLUMNS FROM {table};
        """
        cursor.execute(sql)
        for i in cursor.fetchall():
            row = list(i)
            mapping['field'].append(row[0])
            mapping['type'].append(row[1])
            mapping['null'].append(row[2])
        conn.commit()
        cursor.close()
        conn.close()
        success = True
    elif context == 'sas':
        print("")
        # TODO: Criar logica de conexão com SAS;
        success = True
    elif context == 'hive':
        print("")
        # TODO: Criar lógica de conexão com hive;
        success = True
    else:
        print("")
        # TODO: Criar regra para o caso em não tenham
    return pd.DataFrame(mapping, sep = "|", index=False), success


def export_s3(file: str):
    filename = file.split('/')[-1]
    s3_path = f's3://{bucket}/{filename}'
    wr.s3.upload(local_file=file, 
                 path=s3_path)
    return s3_path


def load_data(def_path: str):
    if def_path == 'csv':
        try:
            df = wr.s3.read_csv(
                def_path,
                encoding='utf-8',
                sep='|',
                keep_default_na=False,
                use_threads=True
            )
            
            if len(list(df.columns)) <= 1:
                print("Only one column found for csv, perfoming fallback read with sep=;")
                df = wr.s3.read_csv(
                    src['s3_uri'],
                    encoding='utf-8',
                    sep=';',
                    keep_default_na=False,
                    use_threads=True                
                )
                
        except pd.errors.ParserError as e:
                print("Parser Error, trying with ; separator.")
                df = wr.s3.read_csv(
                    src['s3_uri'],
                    encoding='utf-8',
                    sep=';',
                    keep_default_na=False,
                    use_threads=True                
                )
    elif src['ext'] == 'parquet':
        try:
            df = wr.s3.read_parquet(
                src['s3_uri'],
                use_threads=True
            )
        except ArrowInvalid as e:
            print(str(e))
            print("fallback read with safe cast = False")
            df = wr.s3.read_parquet(
                src['s3_uri'],
                use_threads=True,
                safe=False
            )
    return ge.from_pandas(df)


if __name__ == '__main__':
    df, success = get_definition(
        context=context,
        table=table
    )
    if success:
        def_path = f'{os.getcwd()}/aws/s3/{prefix}/{table}.def.csv'
        df.to_csv(def_path, sep="|", index=False)
        export_s3(def_path)
    
    