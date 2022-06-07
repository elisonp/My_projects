import psycopg2
import os
def conecta_db(host,port,user,password,database,options):
    con = psycopg2.connect(host= host,
                                port=port,
                                user=user,
                                password=password, 
                                database=database,
                                options=options)
    return con
#Repositório - Data Lake
dl={'host':os.environ.get("data_lake_db_host"),
            'port':os.environ.get("data_lake_db_port"),
            'user':os.environ.get("bd_dl_user"),
            'password':os.environ.get("bd_dl_senha"),
            'database':os.environ.get("data_lake_db_name")}
#Repositório - Data WareHouse
dw={'host':os.environ.get("data_warehouse_db_host"),
            'port':os.environ.get("data_warehouse_db_port"),
            'user':os.environ.get("bd_warehouse_user"),
            'password':os.environ.get("bd_warehouse_senha"),
            'database':os.environ.get("data_warehouse_db_name")}
#Função para conseguir fazer consultas SQL
def Open_connection(schema_bd, repositorio):
        con = conecta_db(repositorio['host'],repositorio['port'], repositorio['user'],repositorio['password'],repositorio['database'],"-c search_path=dbo,"+schema_bd+"")
        return con
#Função para criar tabelas no banco de dados
def criar_db(sql,schema_bd,repositorio):
    con = conecta_db(repositorio['host'],repositorio['port'], repositorio['user'],repositorio['password'],repositorio['database'],"-c search_path=dbo,"+schema_bd+"")
    cur = con.cursor()
    cur.execute(sql)
    print(sql)
    con.commit()
    con.close()
#Função para inserir dados na tabela desejada
def inserir_db(sql, values,schema_bd,repositorio):
    con = conecta_db(repositorio['host'],repositorio['port'], repositorio['user'],repositorio['password'],repositorio['database'],"-c search_path=dbo,"+schema_bd+"")
    cur = con.cursor()
    try:
        cur.execute(sql, values)
        print(values)
        con.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        con.rollback()
        cur.close()
        return 1
    cur.close()
