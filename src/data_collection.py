import requests
import pandas as pd
from datetime import datetime

def fetch_disease_data(disease_code, year, state_code):
    base_url = "http://tabnet.datasus.gov.br/cgi/tabcgi.exe?sinannet/cnv/"
    params = {
        'Linha': 'Município',
        'Coluna': 'Semana Epidemiológica',
        'Incremento': 'Casos confirmados',
        'Arquivos': f'{disease_code}{year}.dbf',
        'SRegião': state_code
    }
    
    response = requests.get(base_url + f"{disease_code}.def", params=params)
    # ... data parsing logic ...
    return data

def save_weekly_data(disease_code, state_code):
    current_year = datetime.now().year
    all_data = pd.DataFrame()
    
    for year in range(2014, current_year + 1):
        yearly_data = fetch_disease_data(disease_code, year, state_code)
        all_data = pd.concat([all_data, yearly_data])
    
    all_data.to_csv(f"../data/raw/{disease_code}_data_{datetime.now().strftime('%Y%m%d')}.csv", index=False)

if __name__ == "__main__":
    # Example for dengue prototype (disease_code: denguebpe, Pernambuco: 26)
    save_weekly_data("denguebpe", "26")