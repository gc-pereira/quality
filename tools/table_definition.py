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

src = {}


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
    
    
def dtypes_from_sql_types():
    dtypes = {}
    null_cols = []
    # read sql types from | csv
    sql_types = sql_types = pd.read_csv(
        'aws/s3/quality/' + '/'.join([src['database'], src['owner'], src['table']+'.def.csv']).lower(),
        sep='|',
        encoding='utf-8',
        keep_default_na=False
    ).to_dict(orient='index')
    col_num = {sql_types[num]['name'] : num for num in sql_types}
    col_list = sorted([col for col in col_num])
    # transform types
    for col in col_list:
        # column dtype conversion
        column = sql_types[col_num[col]]
        message = ''
        if column['type'].upper().startswith('INTEGER'):
            dtypes[column['name']] = "int64"
        elif column['type'].upper().startswith('LONG'):
            dtypes[column['name']] = "object"
        elif column['type'].upper().startswith('NUMBER'):
            nums = re.findall('\d+', column['type'].upper())
            if len(nums) == 0:
                dtypes[column['name']] = 'float64'
            elif len(nums) == 1:
                if int(nums[0]) < 3:
                    dtypes[column['name']] = "int8"
                elif int(nums[0]) < 5:
                    dtypes[column['name']] = "int16"
                elif int(nums[0]) < 10:
                    dtypes[column['name']] = "int32"
                else:
                    dtypes[column['name']] = "int64"
            else:
                dtypes[column['name']] = "float64"
        elif column['type'].upper().startswith('VARCHAR') or \
             column['type'].upper().startswith('NVARCHAR') or \
             column['type'].upper().startswith('CHAR') or \
             column['type'].upper().startswith('DATE') or \
             column['type'].upper().startswith('TIME') or \
             column['type'].upper().startswith('STRING'):
            dtypes[column['name']] = "object"
        elif column['type'].upper().startswith('BLOB') or \
             column['type'].upper().startswith('CLOB'):
            print('WARN !-> '+str(column)+' requires processing')
            dtypes[column['name']] = "object"
        else:
            print('FAIL !-> '+str(column))
            break
        # columns dtype null check
        if dtypes[column['name']].startswith("int") and column['null'].upper() == 'Y':
            dtypes[column['name']] = dtypes[column['name']].capitalize()
        # info print
        print(f"{column['name']:35} ({column['null'].upper():1}) {column['type'].upper():20} ->   {dtypes[column['name']]:7}   "+message)
        if column['null'].upper() == 'Y':
            null_cols.append(column['name'])
    return dtypes, null_cols


if __name__ == '__main__':
    df, success = get_definition(
        context=context,
        table=table
    )
    if success:
        def_path = f'{os.getcwd()}/aws/s3/{prefix}/{table}.def.csv'
        df.to_csv(def_path, sep="|", index=False)
        export_s3(def_path)
        dtypes_from_sql_types(df, def_path)
    
    