from datetime import datetime, date
import pandas as pd
from meteostat import Point, Daily
import geobr
import warnings
import unicodedata

# Ignorar warnings do Meteostat (opcional, para limpar a saída)
warnings.filterwarnings("ignore", category=UserWarning, module="meteostat")

def normalize_text(text):
    """Normaliza texto para remover problemas de codificação."""
    if isinstance(text, str):
        # Normalizar para forma NFKD e converter para ASCII, removendo acentos incorretos
        return unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('ASCII')
    return text

def get_municipalities_by_state(state_input):
    """Retorna lista de municípios para o estado informado (nome ou sigla)."""
    # Carregar dados de todos os municípios brasileiros
    municipalities = geobr.read_municipality(year=2020)
    
    # Dicionário de siglas para nomes completos dos estados
    state_dict = {
        'AC': 'Acre', 'AL': 'Alagoas', 'AP': 'Amapá', 'AM': 'Amazonas',
        'BA': 'Bahia', 'CE': 'Ceará', 'DF': 'Distrito Federal', 'ES': 'Espírito Santo',
        'GO': 'Goiás', 'MA': 'Maranhão', 'MT': 'Mato Grosso', 'MS': 'Mato Grosso do Sul',
        'MG': 'Minas Gerais', 'PA': 'Pará', 'PB': 'Paraíba', 'PR': 'Paraná',
        'PE': 'Pernambuco', 'PI': 'Piauí', 'RJ': 'Rio de Janeiro', 'RN': 'Rio Grande do Norte',
        'RS': 'Rio Grande do Sul', 'RO': 'Rondônia', 'RR': 'Roraima', 'SC': 'Santa Catarina',
        'SP': 'São Paulo', 'SE': 'Sergipe', 'TO': 'Tocantins'
    }
    
    # Normalizar entrada do estado
    state_input = state_input.strip().title()
    if state_input in state_dict.values():
        state_name = state_input
    elif state_input.upper() in state_dict:
        state_name = state_dict[state_input.upper()]
    else:
        raise ValueError("Estado não encontrado. Use nome completo ou sigla (ex.: 'PE' ou 'Pernambuco').")
    
    # Filtrar municípios do estado
    municipalities = municipalities[municipalities['name_state'] == state_name]
    
    if municipalities.empty:
        raise ValueError(f"Nenhum município encontrado para o estado {state_name}.")
    
    # Normalizar nomes dos municípios
    municipalities['name_muni'] = municipalities['name_muni'].apply(normalize_text)
    
    return municipalities

def collect_weather_data(state_input, start_date, end_date):
    """Coletar dados meteorológicos para todos os municípios de um estado."""
    # Obter lista de municípios
    municipalities = get_municipalities_by_state(state_input)
    
    # Criar DataFrame vazio para armazenar todos os dados
    all_data = pd.DataFrame()
    
    # Iterar sobre os municípios
    for _, row in municipalities.iterrows():
        city_name = row['name_muni']
        code_muni = row['code_muni']
        geometry = row['geometry']
        centroid = geometry.centroid
        latitude = centroid.y
        longitude = centroid.x
        
        print(f"Coletando dados para {city_name} ({code_muni})...")
        
        # Criar objeto Point para o Meteostat
        point = Point(latitude, longitude)
        
        # Obter dados diários
        try:
            data = Daily(point, start_date, end_date).fetch()
            if not data.empty:
                # Adicionar colunas de identificação
                data['city'] = city_name
                data['code_muni'] = code_muni
                data['state'] = row['name_state']
                # Preencher valores NaN com 0 para colunas numéricas
                numeric_columns = ['tavg', 'tmin', 'tmax', 'prcp', 'wspd', 'pres', 'tsun']
                data[numeric_columns] = data[numeric_columns].fillna(0)
                all_data = pd.concat([all_data, data])
            else:
                print(f"Aviso: Nenhum dado disponível para {city_name}.")
        except Exception as e:
            print(f"Erro ao coletar dados para {city_name}: {e}")
    
    # Resetar índice e renomear coluna de data
    all_data = all_data.reset_index().rename(columns={'index': 'date'})
    
    return all_data

def main():
    # Configurar período de tempo (de 2021 até hoje)
    start = datetime(2021, 1, 1)
    end = datetime.now()  # Data atual
    
    # Solicitar estado do usuário
    state_input = input("Digite o nome ou sigla do estado (ex.: 'PE' ou 'Pernambuco'): ")
    
    # Coletar dados
    try:
        weather_data = collect_weather_data(state_input, start, end)
        
        # Exportar para CSV com codificação UTF-8-SIG
        state_name = state_input.upper() if len(state_input) <= 2 else state_input.title()
        output_file = f"{state_name.replace(' ', '_')}_weather_data_2021_to_now.csv"
        weather_data.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"Dados exportados para {output_file}")
        
        # Exibir resumo
        print(f"\nResumo dos dados coletados:")
        print(weather_data[['date', 'city', 'tavg', 'prcp']].head())
        
    except ValueError as e:
        print(f"Erro: {e}")
    except Exception as e:
        print(f"Erro inesperado: {e}")

if __name__ == "__main__":
    main()