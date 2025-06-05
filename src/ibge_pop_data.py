import requests
import pandas as pd
import unicodedata
import warnings
import os
import re
from datetime import datetime

# Ignorar warnings
warnings.filterwarnings("ignore", category=UserWarning)

def normalize_text(text):
    """Normaliza texto para remover acentos e espaços."""
    if isinstance(text, str):
        return unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('ASCII').replace(' ', '_')
    return text

def get_state_code(state_input):
    """Retorna o código da UF com base no nome ou sigla do estado."""
    state_dict = {
        'AC': ('Acre', '12'), 'AL': ('Alagoas', '27'), 'AP': ('Amapá', '16'), 'AM': ('Amazonas', '13'),
        'BA': ('Bahia', '29'), 'CE': ('Ceará', '23'), 'DF': ('Distrito Federal', '53'), 'ES': ('Espírito Santo', '32'),
        'GO': ('Goiás', '52'), 'MA': ('Maranhão', '21'), 'MT': ('Mato Grosso', '51'), 'MS': ('Mato Grosso do Sul', '50'),
        'MG': ('Minas Gerais', '31'), 'PA': ('Pará', '15'), 'PB': ('Paraíba', '25'), 'PR': ('Paraná', '41'),
        'PE': ('Pernambuco', '26'), 'PI': ('Piauí', '22'), 'RJ': ('Rio de Janeiro', '33'), 'RN': ('Rio Grande do Norte', '24'),
        'RS': ('Rio Grande do Sul', '43'), 'RO': ('Rondônia', '11'), 'RR': ('Roraima', '14'), 'SC': ('Santa Catarina', '42'),
        'SP': ('São Paulo', '35'), 'SE': ('Sergipe', '28'), 'TO': ('Tocantins', '17')
    }
    
    state_input = state_input.strip().title()
    if state_input in [v[0] for v in state_dict.values()]:
        for sigla, (nome, codigo) in state_dict.items():
            if state_input == nome:
                return sigla, nome, codigo
    elif state_input.upper() in state_dict:
        sigla = state_input.upper()
        return sigla, state_dict[sigla][0], state_dict[sigla][1]
    else:
        raise ValueError("Estado não encontrado. Use nome completo ou sigla (ex.: 'PE' ou 'Pernambuco').")

def get_municipios_codes(state_input):
    """Obter códigos e nomes de municípios para um estado via API do IBGE."""
    sigla, state_name, state_code = get_state_code(state_input)
    url = f"https://servicodados.ibge.gov.br/api/v1/localidades/estados/{state_code}/municipios"
    
    print(f"\nObtendo códigos de municípios para {state_name}...")
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        municipios = []
        for item in data:
            codigo = str(item['id'])
            nome = normalize_text(item['nome'])
            municipios.append({'TERCODIGO': codigo, 'Municipio': nome})
        
        df_municipios = pd.DataFrame(municipios)
        return df_municipios
    
    except requests.exceptions.RequestException as e:
        print(f"Erro na requisição de códigos de municípios: {e}")
        return None

def collect_from_api(state_input, year):
    """Coletar dados de população via API do IBGE."""
    sigla, state_name, state_code = get_state_code(state_input)
    url = f"https://servicodados.ibge.gov.br/api/v3/agregados/6579/periodos/{year}/variaveis/9324?localidades=N6[all]"
    
    print(f"\nColetando dados de população para {state_name} em {year} via API...")
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if not data or not data[0].get('resultados'):
            raise ValueError(f"Resultados vazios para {year} na API.")
        
        resultados = []
        for item in data[0]['resultados'][0]['series']:
            codigo = str(item['localidade']['id'])
            municipio = normalize_text(item['localidade']['nome'].split(' - ')[0])
            populacao = int(item['serie'][year])
            resultados.append({'TERCODIGO': codigo, 'Municipio': municipio, 'Populacao': populacao})
        
        df = pd.DataFrame(resultados)
        df_state = df[df['TERCODIGO'].str.startswith(state_code)]
        
        if df_state.empty:
            raise ValueError(f"Nenhum dado encontrado para {state_name} em {year}.")
        
        print(f"Encontrados {len(df_state)} municípios para {state_name} em {year}.")
        return df_state
    
    except requests.exceptions.RequestException as e:
        print(f"Erro na requisição para {year}: {e}")
        return None
    except (KeyError, ValueError) as e:
        print(f"Erro ao processar dados para {year}: {e}")
        return None

def process_2022_csv(state_input, file_path="tabela2022.csv"):
    """Processar tabela2022.csv (Censo 2022)."""
    sigla, state_name, state_code = get_state_code(state_input)
    
    print(f"\nProcessando dados de população para {state_name} em 2022 via {file_path}...")
    try:
        # Ler CSV, ignorando linhas de cabeçalho irrelevantes
        df = pd.read_csv(file_path, sep=';', encoding='utf-8', skiprows=5)
        
        # Renomear colunas
        df.columns = ['Municipio', 'Forma_Declaracao', 'Populacao']
        
        # Filtrar linhas onde Forma_Declaracao é "Total"
        df = df[df['Forma_Declaracao'].str.strip() == 'Total']
        
        # Limpar nome do município (remover UF)
        df['Municipio_Norm'] = df['Municipio'].apply(lambda x: re.sub(r'\s*\(.*\)', '', x).strip())
        df['Municipio_Norm'] = df['Municipio_Norm'].apply(normalize_text)
        df['Populacao'] = pd.to_numeric(df['Populacao'], errors='coerce').fillna(0).astype(int)
        
        # Filtrar por estado usando a UF no nome original
        df = df[df['Municipio'].str.contains(f'\({sigla}\)', na=False)]
        
        # Obter códigos de municípios
        df_municipios = get_municipios_codes(state_input)
        if df_municipios is None:
            raise ValueError("Não foi possível obter códigos de municípios para mapear TERCODIGO.")
        
        # Mapear TERCODIGO com base no nome normalizado
        df = df.merge(df_municipios, left_on='Municipio_Norm', right_on='Municipio', how='left')
        
        # Verificar se há correspondências
        if df['TERCODIGO'].isna().all():
            print(f"Nenhum município de {state_name} encontrado em tabela2022.csv.")
            return None
        
        df_state = df[['TERCODIGO', 'Municipio_Norm', 'Populacao']].rename(columns={'Municipio_Norm': 'Municipio'})
        
        if df_state.empty:
            print(f"Nenhum dado encontrado para {state_name} em 2022 após mapeamento.")
            return None
        
        print(f"Encontrados {len(df_state)} municípios para {state_name} em 2022.")
        return df_state
    
    except Exception as e:
        print(f"Erro ao processar tabela2022.csv: {e}")
        return None

def process_2023_csv(state_input, file_path="tabela2023.csv"):
    """Processar tabela2023.csv (Estimativas 2024) com 3 colunas."""
    sigla, state_name, state_code = get_state_code(state_input)
    
    print(f"\nProcessando dados de população para {state_name} em 2023 via {file_path}...")
    try:
        # Ler CSV, pulando a linha de título
        df = pd.read_csv(file_path, encoding='utf-8', skiprows=1)
        
        # Verificar número de colunas
        if len(df.columns) != 3:
            raise ValueError(f"Formato inesperado em {file_path}. Esperado 3 colunas, encontrado {len(df.columns)}.")
        
        # Renomear colunas
        df.columns = ['UF', 'Municipio', 'Populacao']
        
        # Debug: Exibir colunas e UFs disponíveis
        print(f"Colunas lidas em {file_path}: {list(df.columns)}")
        df['UF'] = df['UF'].str.strip('()')  # Remover parênteses (ex.: (RO) -> RO)
        unique_ufs = df['UF'].unique()
        print(f"UF disponíveis em {file_path}: {unique_ufs}")
        if sigla not in unique_ufs:
            raise ValueError(f"Estado {state_name} (UF={sigla}) não encontrado em {file_path}.")
        
        # Limpar população (remover vírgulas, espaços)
        df['Populacao'] = df['Populacao'].str.replace(r'[,]', '', regex=True).str.strip()
        df['Populacao'] = pd.to_numeric(df['Populacao'], errors='coerce').fillna(0).astype(int)
        
        # Normalizar nome do município
        df['Municipio'] = df['Municipio'].apply(normalize_text)
        
        # Filtrar por estado
        df = df[df['UF'] == sigla]
        
        # Obter códigos de município
        df_municipios = get_municipios_codes(state_input)
        if df_municipios is None:
            raise ValueError("Não foi possível obter códigos de municípios para mapear TERCODIGO.")
        
        # Mapear TERCODIGO com base no nome normalizado
        df = df.merge(df_municipios, on='Municipio', how='left')
        
        # Verificar correspondências
        if df['TERCODIGO'].isna().all():
            print(f"Nenhum município de {state_name} encontrado em tabela2023.csv após mapeamento.")
            return None
        
        df_state = df[['TERCODIGO', 'Municipio', 'Populacao']]
        
        if df_state.empty:
            print(f"Nenhum dado encontrado para {state_name} em 2023.")
            return None
        
        print(f"Encontrados {len(df_state)} registros para {state_name} em 2023.")
        return df_state
    
    except Exception as e:
        print(f"Erro ao processar tabela2023.csv: {e}")
        return None

def save_to_csv(df_state, state_name, year, output_dir):
    """Salvar dados em um único CSV por ano dentro da pasta especificada."""
    if df_state is None or df_state.empty:
        return 0
    
    # Criar pasta se não existir
    os.makedirs(output_dir, exist_ok=True)
    
    # Criar DataFrame com todos os municípios
    output_file = os.path.join(output_dir, f"populacao_{state_name}_{year}.csv")
    df_state.to_csv(output_file, columns=['TERCODIGO', 'Municipio', 'Populacao'], index=False, encoding='utf-8-sig')
    
    print(f"\nArquivo gerado: {output_file}")
    print(f"Total de registros para {year}: {len(df_state)}")
    print(f"\nResumo dos dados coletados para {state_name} em {year}:")
    print(df_state[['TERCODIGO', 'Municipio', 'Populacao']].head(5))
    return len(df_state)

def main():
    # Solicitar entrada
    state_input = input("Digite o estado (ex.: 'PE' ou 'Pernambuco'): ")
    years = ['2021', '2022', '2023', '2024']
    
    # Verificar arquivos locais
    for file in ['tabela2022.csv', 'tabela2023.csv']:
        if not os.path.exists(file):
            print(f"Arquivo {file} não encontrado na pasta atual.")
            return
    
    # Criar pasta de saída com timestamp
    try:
        _, state_name, _ = get_state_code(state_input)
    except ValueError as e:
        print(f"Erro: {e}")
        return
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    output_dir = f"populacao_{state_name.replace(' ', '_').replace('Í', 'I').replace('Á', 'A')}_{timestamp}"
    
    print(f"\nCriando pasta para CSVs: {output_dir}")
    
    # Processar cada ano
    for year in years:
        df_state = None
        if year == '2022':
            df_state = process_2022_csv(state_input)
        elif year == '2023':
            df_state = process_2023_csv(state_input)
        else:
            df_state = collect_from_api(state_input, year)
        
        # Validação e salvamento
        if df_state is not None and not df_state.empty:
            if all(df_state['Populacao'] > 0):
                print(f"Dados válidos para {year}.")
            else:
                print(f"Aviso: Alguns valores de população inválidos (<= 0) para {year}.")
            save_to_csv(df_state, state_name.replace(' ', '_').replace('Í', 'I'), year, output_dir)
        else:
            print(f"Erro: Falha ao coletar dados para {year}. Verifique os dados de entrada ou a API.")

if __name__ == "__main__":
    main()