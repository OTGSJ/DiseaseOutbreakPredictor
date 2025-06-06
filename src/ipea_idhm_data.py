import requests
import pandas as pd
import unicodedata

def normalize_text(text):
    """Normaliza texto para remover problemas de codificação."""
    if isinstance(text, str):
        return unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('ASCII')
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

def get_municipio_nome(tercodigo, municipios_cache=None):
    """Obtém o nome do município a partir do TERCODIGO usando a API do IBGE."""
    if municipios_cache is None:
        try:
            url_municipios = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
            response = requests.get(url_municipios)
            response.raise_for_status()
            municipios_cache = response.json()
        except requests.exceptions.RequestException as e:
            print(f"Erro ao acessar API do IBGE: {e}")
            return "Erro ao obter nome"
    
    for municipio in municipios_cache:
        if str(municipio['id']) == tercodigo:
            return normalize_text(municipio['nome'])
    return "Desconhecido"

def collect_idhm_data(state_input):
    """Coletar dados de IDHM para os municípios do estado informado."""
    # Obter sigla, nome e código do estado
    sigla, state_name, state_code = get_state_code(state_input)
    
    # URL para metadados do Ipeadata
    url_metadata = "http://www.ipeadata.gov.br/api/odata4/Metadados"
    
    try:
        # Verificar séries disponíveis
        response_metadata = requests.get(url_metadata)
        response_metadata.raise_for_status()
        metadata = response_metadata.json()
        
        # Confirmar que a série IDHM existe
        series = [s for s in metadata['value'] if s['SERCODIGO'] == 'IDHM']
        if not series:
            raise ValueError("Série IDHM não encontrada nos metadados.")
        
        print(f"Série IDHM confirmada para {state_name} (código {state_code}).")
        
        # URL para valores da série IDHM (sem filtros)
        url_valores = "http://www.ipeadata.gov.br/api/odata4/ValoresSerie(SERCODIGO='IDHM')"
        
        # Fazer a requisição para valores
        response_valores = requests.get(url_valores)
        response_valores.raise_for_status()
        data = response_valores.json()
        
        # Cache dos municípios do IBGE para evitar múltiplas chamadas
        url_municipios = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
        response_municipios = requests.get(url_municipios)
        response_municipios.raise_for_status()
        municipios_cache = response_municipios.json()
        
        # Lista para armazenar resultados
        resultados = []
        
        # Processar os dados
        for item in data['value']:
            ano = item['VALDATA'][:4]
            nivel = item['NIVNOME']
            tercodigo = item['TERCODIGO']
            idhm = float(item['VALVALOR'])
            resultados.append({'Ano': ano, 'NIVNOME': nivel, 'TERCODIGO': tercodigo, 'IDHM': idhm})
        
        # Criar DataFrame
        df = pd.DataFrame(resultados)
        
        # Filtrar para o estado (TERCODIGO começando com o código da UF), ano 2010 e nível Municípios
        df_state = df[
            (df['TERCODIGO'].str.startswith(state_code)) &
            (df['Ano'] == '2010') &
            (df['NIVNOME'] == 'Municípios')
        ]
        
        # Verificar se há dados
        if df_state.empty:
            raise ValueError(f"Nenhum dado encontrado para {state_name} em 2010 (Municípios). A série IDHM pode não conter dados para este ano.")
        
        # Mapear TERCODIGO para nomes de municípios
        df_state['Municipio'] = df_state['TERCODIGO'].apply(lambda x: get_municipio_nome(x, municipios_cache))
        
        # Verificar valores de IDHM
        if all(df_state['IDHM'].between(0.4, 0.9)):
            print("Dados de IDHM validados com sucesso.")
        else:
            print("Aviso: Alguns valores de IDHM estão fora do intervalo esperado (~0.4–0.9).")
        
        # Selecionar colunas relevantes
        df_state = df_state[['Municipio', 'IDHM']].rename(columns={'IDHM': 'IDHM_2010'})
        
        # Ordenar por município
        df_state = df_state.sort_values(by='Municipio')
        
        # Exportar para CSV
        output_file = f"{state_name.replace(' ', '_')}_idhm_2010.csv"
        df_state.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"Dados exportados para {output_file}")
        print(f"Número de municípios encontrados: {len(df_state)}")
        
        # Exibir resumo
        print(f"\nResumo dos dados coletados para {state_name}:")
        print(df_state.head())
        
        return df_state
    
    except requests.exceptions.HTTPError as e:
        print(f"Erro na requisição à API do Ipeadata: {e}")
        print("Possível causa: endpoint inválido ou série indisponível.")
        print("Tente a alternativa manual do Atlas Brasil.")
        return None
    except KeyError as e:
        print(f"Erro ao processar os dados: {e}")
        return None
    except ValueError as e:
        print(f"Erro: {e}")
        return None

def main():
    # Solicitar estado do usuário
    state_input = input("Digite o nome ou sigla do estado (ex.: 'PE' ou 'Pernambuco'): ")
    
    # Coletar dados
    try:
        idhm_data = collect_idhm_data(state_input)
        if idhm_data is not None:
            print("Coleta de dados concluída com sucesso.")
        else:
            print("Falha na coleta de dados. Considere usar o Atlas Brasil.")
    except Exception as e:
        print(f"Erro inesperado: {e}")

if __name__ == "__main__":
    main()