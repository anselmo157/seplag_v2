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


def update_associate(associado, seplag):
    if not seplag:
        print('Não tem dado')
    else:
        print(seplag)


if __name__ == '__main__':
    sql_associados = """select * from associados
    join orgaoaverbadores ON orgaoaverbadores.id_orgaoaverbador = associados.orgaoaverbador_id
    order by nome_associado 
    """

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

        update_associate(associados[i], query_seplag)
