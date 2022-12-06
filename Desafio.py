#Desafio
#bibliotecas usadas para: Web Scrap, conexão com MySql e data/hora.
import mysql.connector
from bs4 import BeautifulSoup
import requests
import datetime
import re

# função usada para fazer a conexão, criar o banco de dados e realizar o scrap.
def main():
    conexao = mysql.connector.connect(
        user='root',
        password='',
        host='localhost',
        database='db_produto')              #apos criar o database lembrar de colocar entre as aspas simples na linha 14! Ex: database = 'db_exemplo'.
    print("Conexão:", conexao)
    cursor = conexao.cursor()

    #cria_db(cursor)               #etapa 1 para uso do bot: para realizar a criação de tabela comentar a linha.
    #cria_tabela(cursor)           #19(cria_tabela), 20(scrap), 21(print_historico) rodar o programa.
    scrap(cursor, conexao)        #etapa 2: comente a linha 18(cria_db), e descomente a linha 19(cria_tabela) e rode o programa.
    print_historico(cursor)       #etapa 3: comente a linha 18 novamente, e descomente a linha 20 e 21 e rode o programa.
                                  # feito essas etapas sera criado o banco de dados, sera feita a raspagem de dados e criaçaõ do historico.
    cursor.close()
    conexao.close()


def cria_db(cursor):   #criação do banco de dados no MySql.
    sql = "CREATE DATABASE IF NOT EXISTS db_produto"
    cursor.execute(sql)


def cria_tabela(cursor):   #criação das tabelas no MySql.
    sql = '''
        CREATE TABLE IF NOT EXISTS tb_produto (
            id INT NOT NULL AUTO_INCREMENT,
            nome VARCHAR(255),
            PRIMARY KEY (id)       
        );
        CREATE TABLE IF NOT EXISTS  tb_registro (
            id INT NOT NULL AUTO_INCREMENT,
            data_registro DATETIME,
            preco VARCHAR(255),
            PRIMARY KEY (id)
        );
        CREATE TABLE IF NOT EXISTS  produto_has_registro (
            produto_id INT NOT NULL,
            registro_id INT NOT NULL,
            PRIMARY KEY (produto_id, registro_id),
            CONSTRAINT produto_has_registro_produto FOREIGN KEY (produto_id)
            REFERENCES tb_produto(id),
            CONSTRAINT produto_has_registro_registro FOREIGN KEY (registro_id)
            REFERENCES tb_registro(id)
        );
        '''

    cursor.execute(sql)


def scrap(cursor, conexao):  #função utilizada para realizar a raspagem de dados.
    URLS = [
        'https://www.pichau.com.br/ssd-mancer-blink-x-512gb-sata-iii-6gb-s-leitura-530-mb-s-gravacao-480-mb-s-mcr-blkx1-512',
        'https://www.pichau.com.br/gabinete-gamer-mancer-crusader-mid-tower-lateral-de-vidro-com-1-fan-preto-mcr-csd-bk01',
        'https://www.pichau.com.br/monitor-gamer-pichau-cepheus-vpro24-24-5-pol-ips-fhd-1ms-360hz-freesync-pg-cfvpro24-bl01']
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36 OPR/87.0.4390.58"}

    for URL in URLS:  #loop feito para realizar o loop entre as urls podendo ser feita a verificaçaõ de mais de um produto ao mesmo tempo
        site = requests.get(URL, headers=headers)
        soup = BeautifulSoup(site.content, 'html.parser')

        title = soup.find("h1").get_text()

        prices = soup.find_all('div')
        price_tag = [p for p in prices if 'R$' in p][0]
        price = re.search(r'\d{3}\.\d{2}', str(price_tag)).group(0)

        produtos = get_prod_por_nome(title, cursor)

        if len(produtos) == 0:
            sql1 = """INSERT INTO tb_produto(nome) VALUES (%s)"""
            cursor.execute(sql1, [title])
            cursor.execute('SELECT last_insert_id()')
            produto_id = cursor.fetchone()[0]
            registrar_preco(cursor, conexao, produto_id, price)
            conexao.commit()
        else:
            produto_id = produtos[0][0]
            registrar_preco(cursor, conexao,produto_id,price)

def registrar_preco(cursor,conexao,produto_id,price): #função utilizada para registrar o preços dos produtos, registrando a data e hora no registro.
    sql2 = """INSERT INTO tb_registro(data_registro, preco) VALUES (%s, %s)"""
    cursor.execute(sql2, [datetime.datetime.now(), price])
    cursor.execute('SELECT last_insert_id()')
    registro_id = cursor.fetchone()[0]
    sql3 = """INSERT INTO produto_has_registro(produto_id, registro_id) VALUES (%s,%s)"""
    cursor.execute(sql3, [produto_id, registro_id])
    conexao.commit()

def get_prod_por_nome(prod_nome, cursor): #função que cham os produtos por nome.
    sql = "SELECT * FROM tb_produto WHERE nome LIKE %s "
    cursor.execute(sql, ["%" + prod_nome + "%"])
    records = cursor.fetchall()
    return records

def print_historico(cursor): #função usada para imprimir o historico.
  sql = """SELECT p.nome, DATE_FORMAT(r.data_registro, '%d/%m/%Y %H:%i:%s'), r.preco FROM tb_produto p 
    JOIN tb_registro r 
    JOIN produto_has_registro phr 
    WHERE phr.produto_id = p.id AND phr.registro_id = r.id"""
  cursor.execute(sql)
  registros = cursor.fetchall()
  for registro in registros:
    print(registro)

main()
