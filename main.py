import psycopg2
import pandas as pd


def connect_db():
    connection = psycopg2.connect(host='localhost',
                                  database='cabemce',
                                  user='martins',
                                  password='34784575')
    return connection


def query_db(sql_query):
    con = connect_db()
    cur = con.cursor()
    cur.execute(sql_query)
    aux_query = cur.fetchall()
    registers = []
    for rec in aux_query:
        registers.append(rec)
    con.close()

    return registers


def execute_sql(sql, values, return_id=False):
    con = connect_db()
    cur = con.cursor()

    aux_sql = None

    try:
        cur.execute(sql, values)
        if return_id:
            aux_sql = cur.fetchall()
        con.commit()
    except(Exception, psycopg2.DatabaseError) as error:
        print('Error: %s' % error)
        con.rollback()
        cur.close()
        return 1

    cur.close()

    if return_id:
        return aux_sql[0][0]


def update_cpf(value, associado_id):
    sql = """update associados set cpf = %s where id_associado = %s"""
    sql_values = (value, associado_id)
    execute_sql(sql, sql_values)


def update_matricula(value, associado_id):
    sql = """update associados set matricula = %s where id_associado = %s"""
    sql_values = (value, associado_id)
    execute_sql(sql, sql_values)


def update_orgao(value, associado_id):
    sql = """update associados set orgaoaverbador_id = %s where id_associado = %s"""
    sql_values = (value, associado_id)
    execute_sql(sql, sql_values)


def update_folha(value, associado_id):
    sql = """update associados set folhapagamento_id = %s where id_associado = %s"""
    sql_values = (value, associado_id)
    execute_sql(sql, sql_values)


def update_cargo(value, associado_id):
    sql = """update associados set postograduacao_id = %s where id_associado = %s"""
    sql_values = (value, associado_id)
    execute_sql(sql, sql_values)


def update_cidade(value, associado_id):
    sql = """update associados set cidade_id = %s where id_associado = %s"""
    sql_values = (value, associado_id)
    execute_sql(sql, sql_values)

def update_telefone(value, associado_id):
    sql = """update associados set orgaoaverbador_id = %s where id_associado = %s"""
    sql_values = (value, associado_id)
    execute_sql(sql, sql_values)

def update_endereco(value_cep, value_endereco,value_numero, value_complemento, value_bairro, value_municipio,associado_id):
    sql = """update associados set orgaoaverbador_id = %s where id_associado = %s"""
    sql_values = (value_cep, associado_id)
    execute_sql(sql, sql_values)


def update_associate(associado, seplag):
    cpf_associado = associado[3]
    matricula_associado = associado[4]
    email1_associado = associado[5]
    email2_associado = associado[6]
    folha_associado = associado[7]
    orgao_associado = associado[8]
    cargo_associado = associado[10]
    cidade_associado = associado[9]

    cpf_seplag = seplag[1]
    matricula_seplag = seplag[2]
    orgao_seplag = seplag[3]
    folha_seplag = seplag[4]
    cargo_seplag = seplag[5]
    email_seplag = seplag[6]
    telefone = seplag[7]
    cep = seplag[8]
    endereco = seplag[9]
    numero = seplag[10]
    complemento = seplag[11]
    bairro = seplag[12]
    municipio = seplag[13]

    if cpf is None or cpf == '':
        print(cpf_seplag)
        print(associado)


if __name__ == '__main__':
    sql_associados = """select * from associados order by nome_associado"""

    associados = query_db(sql_associados)

    for i in range(len(associados)):
        nome_associado = associados[i][1]
        cpf = associados[i][3]
        matricula = associados[i][4]

        sql_search = """select nome, cpf, matricula, orgao, folha, cargo, email, telefone, cep, endereco, 
                    num, complemento, bairro, municipio from auxiliares.dados_silveira_v1 
                    where (nome = ('%s') and cpf = ('%s')) or (nome = ('%s') and matricula = ('%s'))
                    """ % (nome_associado, cpf, nome_associado, matricula)

        query_seplag = query_db(sql_search)

        if len(query_seplag) > 1:
            first = query_seplag[0]
            second = query_seplag[1]
            if first[1] == second[1] and first[2] == second[2] and first[3] == second[3] and first[4] == second[4] and \
                    first[5] == second[5]:
                update_associate(associados[i], first)
                continue
            else:
                continue

        if not query_seplag:
            print('Não tem dados')
        else:
            update_associate(associados[i], query_seplag[0])
