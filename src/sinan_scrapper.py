import requests
from bs4 import BeautifulSoup
import csv
from urllib.parse import quote_plus
from datetime import date
from epiweeks import Week

# Entradas do usuário
estado_sigla = input("Digite a sigla do estado (ex: PE, SP, RJ): ").strip().upper()
municipio_alvo = input("Digite o nome exato do município (como aparece na base do SINAN): ").strip().upper()

# Função para montar e executar a requisição por ano
def coletar_dados_ano(ano, estado, municipio_filtro):
    sufixo_ano = str(ano)[-2:]  # Ex: 2014 → '14'
    arquivos = f"deng{estado.lower()}{sufixo_ano}.dbf"

    linha = "Município_de_notificação"
    coluna = "Semana_epidem._notificação"

    linha_enc = quote_plus(linha, encoding='iso-8859-1')
    coluna_enc = quote_plus(coluna, encoding='iso-8859-1')
    arquivos_enc = quote_plus(arquivos, encoding='iso-8859-1')

    payload = (
        f"Linha={linha_enc}&"
        f"Coluna={coluna_enc}&"
        f"Incremento=Casos_Prováveis&"
        f"Arquivos={arquivos_enc}&"
        "formato=prn&mostre=Mostra"
    )

    url = f"http://tabnet.datasus.gov.br/cgi/tabcgi.exe?sinannet/cnv/dengueb{estado.lower()}.def"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": "Mozilla/5.0",
    }

    try:
        response = requests.post(url, headers=headers, data=payload.encode("iso-8859-1"))
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"[{ano}] Erro na requisição: {e}")
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    pre_tag = soup.find("pre")
    if not pre_tag:
        print(f"[{ano}] Tabela não encontrada.")
        return []

    table_data = pre_tag.text.strip().splitlines()

    if table_data[-1].endswith('&'):
        table_data[-1] = table_data[-1][:-1]

    semanas = []
    dados = []

    for i, line in enumerate(table_data):
        row = [item.strip('"') for item in line.split(';')]

        if i == 0:
            semanas = row[1:]
        else:
            municipio = row[0].strip().upper()
            if municipio_filtro in municipio:
                valores = row[1:]
                for idx, val in enumerate(valores):
                    semana = semanas[idx].strip().upper()
                    if semana == 'TOTAL':  # Ignora a linha total na coluna semana
                        continue
                    valor = val.strip()
                    if valor == '' or valor == '-':
                        valor = '0'
                    dados.append([ano, semana, municipio, valor])
    print(f"[{ano}] {len(dados)} registros coletados para {municipio_filtro}.")
    return dados


# Obtém o ano e semana epidemiológica atuais
hoje = date.today()
semana_epi = Week.fromdate(hoje)
ano_atual = semana_epi.year

# Coleta e consolida os dados de todos os anos
todos_dados = []
for ano in range(2021, ano_atual + 1):
    dados_ano = coletar_dados_ano(ano, estado_sigla, municipio_alvo)
    todos_dados.extend(dados_ano)

# Verifica se houve dados encontrados
if not todos_dados:
    print(f"\nNenhum dado encontrado para o município '{municipio_alvo}' no estado '{estado_sigla}'. Verifique a grafia.")
else:
    # Salva todos os dados em um único arquivo CSV
    nome_arquivo = f"dengue_{estado_sigla}_{municipio_alvo.replace(' ', '_')}_2021-{ano_atual}.csv"
    with open(nome_arquivo, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file, delimiter=';')
        writer.writerow(["Ano", "Semana", "Municipio", "Casos"])
        writer.writerows(todos_dados)

    print(f"\nArquivo final salvo como: {nome_arquivo}")
    print(f"Total de registros: {len(todos_dados)}")
