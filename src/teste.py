import requests
from bs4 import BeautifulSoup
import csv
from urllib.parse import quote_plus

# Variáveis principais
linha = "Município_de_notificação"
coluna = "Semana_epidem._notificação"
arquivos = "dengpe14.dbf"

# Codifica as variáveis
linha_enc = quote_plus(linha, encoding='iso-8859-1')
coluna_enc = quote_plus(coluna, encoding='iso-8859-1')
arquivos_enc = quote_plus(arquivos, encoding='iso-8859-1')

# Monta o payload (usando formato=prn)
payload = (
    f"Linha={linha_enc}&"
    f"Coluna={coluna_enc}&"
    f"Incremento=Casos_Prováveis&"
    f"Arquivos={arquivos_enc}&"
    "pesqmes1=Digite+o+texto+e+ache+fácil&"
    "SAno_1%BA_Sintoma%28s%29=TODAS_AS_CATEGORIAS__&"
    "pesqmes2=Digite+o+texto+e+ache+fácil&"
    "SM%C3%AAs_1%BA_Sintoma%28s%29=TODAS_AS_CATEGORIAS__&"
    "pesqmes3=Digite+o+texto+e+ache+fácil&"
    "SSemana_epidem._1%BA_Sintomas%28s%29=TODAS_AS_CATEGORIAS__&"
    "pesqmes4=Digite+o+texto+e+ache+fácil&"
    "SAno_notifica%C3%A7%C3%A3o=TODAS_AS_CATEGORIAS__&"
    "pesqmes5=Digite+o+texto+e+ache+fácil&"
    "SM%C3%AAs_notifica%C3%A7%C3%A3o=TODAS_AS_CATEGORIAS__&"
    "pesqmes6=Digite+o+texto+e+ache+fácil&"
    "SSemana_epidem._notifica%C3%A7%C3%A3o=TODAS_AS_CATEGORIAS__&"
    "pesqmes7=Digite+o+texto+e+ache+fácil&"
    "SAno_epidem._notifica%C3%A7%C3%A3o=TODAS_AS_CATEGORIAS__&"
    "pesqmes8=Digite+o+texto+e+ache+fácil&"
    "SAno_epidem._1%BA_Sintomas%28s%29=TODAS_AS_CATEGORIAS__&"
    "pesqmes9=Digite+o+texto+e+ache+fácil&"
    "SMunic%C3%ADpio_de_notifica%C3%A7%C3%A3o=TODAS_AS_CATEGORIAS__&"
    "pesqmes10=Digite+o+texto+e+ache+fácil&"
    "SRegi%C3%A3o_de_Sa%C3%BAde_%28CIR%29_de_notif=TODAS_AS_CATEGORIAS__&"
    "SMacrorreg.de_Sa%C3%BAde_de_notific=TODAS_AS_CATEGORIAS__&"
    "pesqmes12=Digite+o+texto+e+ache+fácil&"
    "SDiv.adm.estadual_de_notific=TODAS_AS_CATEGORIAS__&"
    "pesqmes13=Digite+o+texto+e+ache+fácil&"
    "SMicrorregi%C3%A3o_IBGE_de_notific=TODAS_AS_CATEGORIAS__&"
    "SReg.Metropolit%2FRIDE_de_notific=TODAS_AS_CATEGORIAS__&"
    "pesqmes15=Digite+o+texto+e+ache+fácil&"
    "SMunic%C3%ADpio_de_resid%C3%AAncia=TODAS_AS_CATEGORIAS__&"
    "pesqmes16=Digite+o+texto+e+ache+fácil&"
    "SRegi%C3%A3o_de_Sa%C3%BAde_%28CIR%29_de_resid=TODAS_AS_CATEGORIAS__&"
    "SMacrorreg.de_Sa%C3%BAde_de_resid%C3%AAnc=TODAS_AS_CATEGORIAS__&"
    "pesqmes18=Digite+o+texto+e+ache+fácil&"
    "SDiv.adm.estadual_de_resid%C3%AAncia=TODAS_AS_CATEGORIAS__&"
    "pesqmes19=Digite+o+texto+e+ache+fácil&"
    "SMicrorregi%C3%A3o_IBGE_de_resid%C3%AAnc=TODAS_AS_CATEGORIAS__&"
    "SReg.Metropolit%2FRIDE_de_resid=TODAS_AS_CATEGORIAS__&"
    "SAutoctone_Mun_Res=TODAS_AS_CATEGORIAS__&"
    "pesqmes22=Digite+o+texto+e+ache+fácil&"
    "SPa%C3%ADs_F._infec%C3%A7%C3%A3o=TODAS_AS_CATEGORIAS__&"
    "pesqmes23=Digite+o+texto+e+ache+fácil&"
    "SUF_F.infec%C3%A7%C3%A3o=TODAS_AS_CATEGORIAS__&"
    "pesqmes24=Digite+o+texto+e+ache+fácil&"
    "SMunic%C3%ADpio_infec%C3%A7%C3%A3o=TODAS_AS_CATEGORIAS__&"
    "SCaso_aut%F3ctone_munic_resid=TODAS_AS_CATEGORIAS__&"
    "pesqmes26=Digite+o+texto+e+ache+fácil&"
    "SFaixa_Et%C3%A1ria=TODAS_AS_CATEGORIAS__&"
    "SRa%C3%A7a=TODAS_AS_CATEGORIAS__&"
    "SSexo=TODAS_AS_CATEGORIAS__&"
    "pesqmes29=Digite+o+texto+e+ache+fácil&"
    "SClass._Final=TODAS_AS_CATEGORIAS__&"
    "SCriterio_conf.=TODAS_AS_CATEGORIAS__&"
    "SEvolu%C3%A7%C3%A3o=TODAS_AS_CATEGORIAS__&"
    "SExame_sorol%F3gico_%28IgM%29_Dengue=TODAS_AS_CATEGORIAS__&"
    "SExame_sorologia_Elisa________=TODAS_AS_CATEGORIAS__&"
    "SExame_isolamento_viral_______=TODAS_AS_CATEGORIAS__&"
    "SExame_de_RT-PCR______________=TODAS_AS_CATEGORIAS__&"
    "SSorotipo_____________________=TODAS_AS_CATEGORIAS__&"
    "SExame_de_Histopatologia______=TODAS_AS_CATEGORIAS__&"
    "SExame_de_Imunohistoqu%EDmica___=TODAS_AS_CATEGORIAS__&"
    "SOcorreu_hospitaliza%C3%A7%C3%A3o_______=TODAS_AS_CATEGORIAS__&"
    "formato=prn&mostre=Mostra"
)

url = "http://tabnet.datasus.gov.br/cgi/tabcgi.exe?sinannet/cnv/denguebpe.def"
headers = {
    "Content-Type": "application/x-www-form-urlencoded",
    "User-Agent": "Mozilla/5.0",
}

# Faz a requisição
try:
    response = requests.post(url, headers=headers, data=payload.encode("iso-8859-1"))
    response.raise_for_status()
except requests.RequestException as e:
    print(f"Erro na requisição: {e}")
    exit()

# Parsing do HTML
soup = BeautifulSoup(response.text, "html.parser")

# Extrai o conteúdo da tag <pre> que contém a tabela
pre_tag = soup.find("pre")
if not pre_tag:
    print("Tabela não encontrada no HTML.")
    exit()

# Processa o conteúdo da tag <pre>
table_data = pre_tag.text.strip().splitlines()

# Remove o caractere '&' da última linha, se presente
if table_data[-1].endswith('&'):
    table_data[-1] = table_data[-1][:-1]

# Escreve os dados em um arquivo CSV
output_file = "dengue_table_2014.csv"
with open(output_file, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file, delimiter=';')
    for line in table_data:
        # Divide a linha pelos delimitadores e remove as aspas
        row = [item.strip('"') for item in line.split(';')]
        print(row)
        writer.writerow(row)

print(f"Tabela extraída e salva em {output_file}")