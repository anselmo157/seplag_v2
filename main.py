import psycopg2
from datetime import datetime


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


def update_email1(value, associado_id):
    sql = """update associados set email1 = %s where id_associado = %s"""
    sql_values = (value, associado_id)
    execute_sql(sql, sql_values)


def update_email2(value, associado_id):
    sql = """update associados set email2 = %s where id_associado = %s"""
    sql_values = (value, associado_id)
    execute_sql(sql, sql_values)


def update_orgao(value, associado_id):
    sql_find = """select id_orgaoaverbador from orgaoaverbadores where codigo_orgao_averbador = ('%s')""" % value
    orgaoaverbador_id = query_db(sql_find)
    sql = """update associados set orgaoaverbador_id = %s where id_associado = %s"""
    sql_values = (orgaoaverbador_id[0][0], associado_id)
    execute_sql(sql, sql_values)


def update_folha(value, associado_id):
    sql_find = """select id_folhapagamento from folhapagamentos where codigo_folha = ('%s')""" % value
    folhapagamento_id = query_db(sql_find)
    sql = """update associados set folhapagamento_id = %s where id_associado = %s"""
    sql_values = (folhapagamento_id[0][0], associado_id)
    execute_sql(sql, sql_values)


def update_cargo(value, associado_id):
    sql_find = """select id_postograduacao from postograduacoes where descricao_posto = ('%s')""" % value
    folhapagamento_id = query_db(sql_find)
    sql = """update associados set postograduacao_id = %s where id_associado = %s"""
    sql_values = (folhapagamento_id[0][0], associado_id)
    execute_sql(sql, sql_values)


def update_cidade(value, associado_id):
    sql_cidade = """select id_cidade from cidades where nome_municipio = ('%s')""" % value
    cidade_id = query_db(sql_cidade)
    sql = """update associados set cidade_id = %s where id_associado = %s"""
    sql_values = (cidade_id[0][0], associado_id)
    execute_sql(sql, sql_values)


def update_telefone(value, associado_id):
    ddd = None
    phone = None

    sql_count = """select count(id_telefone) from telefone where associado_id = ('%s')""" % associado_id
    count = query_db(sql_count)

    if count[0][0] == 0:
        if len(value) < 10:
            ddd = '00'
            if len(value) < 8:
                phone = '3' + str(value)
            else:
                phone = str(value)
        else:
            ddd = str(value[:2])
            phone = str(value[2:])

        sql = """insert into public.telefone (numero_telefone, codigo_area, telecomempresa_id, associado_id,
        created, modified) values(%s, %s, %s, %s, %s, %s)"""
        sql_values = (phone, int(ddd), 5, associado_id, datetime.now(), datetime.now())
        execute_sql(sql, sql_values)


def update_endereco(value_cep, value_endereco, value_numero, value_complemento, value_bairro, value_municipio,
                    associado_id):
    cidade_id = None
    sql_count = """select count(id_telefone) from telefone where associado_id = ('%s')""" % associado_id
    count = query_db(sql_count)

    if count[0][0] == 0:
        sql_cidade = """select id_cidade from cidades where nome_municipio = ('%s')""" % value_municipio
        aux = query_db(sql_cidade)
        if len(aux) > 0:
            cidade_id = aux[0][0]

        sql = """insert into public.enderecos (cep, logradouro, numero, complemento, bairro, cidade_id, associado_id,
        denominacao, created, modified) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        sql_values = (value_cep, value_endereco, value_numero, value_complemento, value_bairro, cidade_id, associado_id,
                      'Endereco Importação', datetime.now(), datetime.now())
        execute_sql(sql, sql_values)


def update_associate(associado, seplag):
    id_associado = associado[0]
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

    if cpf_associado is None or cpf_associado == '':
        update_cpf(cpf_seplag, id_associado)

    if matricula_associado is None or matricula_associado == '':
        update_matricula(matricula_seplag, id_associado)

    if email1_associado is None or email1_associado == '':
        update_email1(email_seplag, id_associado)
    else:
        if (email2_associado is None or email2_associado == '') and email_seplag != email1_associado:
            update_email2(email_seplag, id_associado)

    if orgao_associado is None and orgao_associado == '':
        update_orgao(orgao_seplag, id_associado)

    if folha_associado is None and folha_associado == '':
        update_folha(folha_seplag, id_associado)

    if cargo_associado is None and cargo_associado == '':
        update_cargo(cargo_seplag, id_associado)

    if cidade_associado is None and cidade_associado == '':
        update_cidade(municipio, id_associado)

    if telefone is not None and telefone != '':
        update_telefone(telefone, id_associado)

    if endereco is not None and endereco != '':
        update_endereco(cep, endereco, numero, complemento, bairro, municipio, id_associado)


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
                print(query_seplag)
                update_associate(associados[i], first)
                continue
            else:
                continue

        if not query_seplag:
            print('Não tem dados')
        else:
            print(query_seplag)
            update_associate(associados[i], query_seplag[0])
