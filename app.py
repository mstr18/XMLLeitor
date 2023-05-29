from flask import Flask, request, send_file, render_template
import sqlite3
import xml.etree.ElementTree as ET
import pandas as pd
import threading
import math
import io
import pyodbc

app = Flask(__name__)

#connection_string = "Driver={Oracle ODBC Driver};DBQ=10.1.0.131:1521/DBTOTVSP;UID=RM;PWD=?????"
connection_string = "DSN=DBTOTVSP;UID=RM;PWD=f/M701iv_LoAE1@"
connection = pyodbc.connect(connection_string)
cursor = connection.cursor()
cursor.execute("SELECT * FROM SZCID")

rows = cursor.fetchall()
for row in rows:
    print(row)

# Ler arquivo Excel
df = pd.read_excel('UNIMED.xlsx')

# Conectar ao banco SQLite
def get_sqlite_connection():
    # Verifique se já existe um objeto de conexão SQLite para esta thread
    if not hasattr(threading.current_thread(), "sqlite_conn"):
        # Se não houver, crie um novo objeto de conexão SQLite
        threading.current_thread().sqlite_conn = sqlite3.connect("database.db")
    # Retorne o objeto de conexão SQLite para esta thread
    return threading.current_thread().sqlite_conn

# Salvar DataFrame como tabela no banco SQLite
with get_sqlite_connection() as conn:
    df.to_sql('tabela', conn, if_exists='replace')

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload():
    # Obtenha o arquivo XML enviado pelo usuário
    xml_file = request.files['file']

    # Ler o arquivo XML e encontre todas as ocorrências da tag especificada
    tree = ET.parse(xml_file)
    root = tree.getroot()
    ns = {'tiss': 'http://www.ans.gov.br/padroes/tiss/schemas'}
    tags_procuradas = root.findall('.//tiss:codigoProcedimento', ns)

    # Verifique se a tag foi encontrada no arquivo XML
    if len(tags_procuradas) > 0:
        # Abra uma conexão com o banco de dados
        with get_sqlite_connection() as conn:
            cursor = conn.cursor()

            for tag_encontrada in tags_procuradas:
                valor_procurado = tag_encontrada.text

                # Compare a tag com a tabela SQLite
                cursor.execute("SELECT * FROM tabela WHERE A=?", (valor_procurado,))
                resultado = cursor.fetchone()

                # Se a tag existir na tabela, atualize o valor da tag no XML
                if resultado is not None:
                    valor_novo = resultado[2]
                    tag_encontrada.text = str(math.trunc(valor_novo))

            # Salve o arquivo XML com as alterações
            output = io.BytesIO()
            tree.write(output, encoding="utf-8", xml_declaration=True)
            output.seek(0)

            # Feche a conexão com o banco de dados
            conn.commit()
            cursor.close()

            return send_file(output, mimetype='application/xml', as_attachment=True, download_name= xml_file.filename)

    else:
        mensagem = f'A tag {ns["tiss"]}:codigoProcedimento não foi encontrada no arquivo XML.'

    # Exiba uma mensagem na página para informar o resultado da operação
    return mensagem

if __name__ == '__main__':
    app.run(debug=True)
