import polars as pl
import openpyxl
import os
import glob

def extrair_dados_xlsx(caminho_arquivo):
    # Extrair nome da cidade
    wb = openpyxl.load_workbook(caminho_arquivo, data_only=True)
    ws = wb.active
    nome_cidade = ws['B5'].value
    
    # Ler dados com cabeçalho na linha 7
    colunas_teste = pl.read_excel(
        caminho_arquivo,
        read_options={"header_row": 7}
    )
    
    # Obter nome da coluna B
    coluna_b = colunas_teste.columns[1]
    
    # Especificar colunas desejadas: coluna B e colunas de meses
    colunas_meses = ['JAN', 'FEV', 'MAR', 'ABR', 'MAI', 'JUN', 'JUL', 'AGO', 'SET', 'OUT', 'NOV', 'DEZ']
    colunas_para_selecionar = [coluna_b] + colunas_meses
    
    # Selecionar as linhas 9 a 27 (índices 0 a 18 após ler com cabeçalho na linha 7)
    dados_tabela = colunas_teste.slice(0, 19).select(colunas_para_selecionar)
    
    # Adicionar coluna com o nome da cidade
    dados_tabela = dados_tabela.with_columns(pl.lit(nome_cidade).alias("CIDADE"))
    
    return {
        'nome_cidade': nome_cidade,
        'dados_tabela': dados_tabela
    }

def processar_todos_arquivos(pasta_dados):
    # Encontrar arquivos xlsx
    padrao_busca = os.path.join(pasta_dados, "*.xlsx")
    arquivos_xlsx = glob.glob(padrao_busca)
    
    if not arquivos_xlsx:
        print(f"Nenhum arquivo .xlsx encontrado na pasta {pasta_dados}")
        return None
    
    print(f"Encontrados {len(arquivos_xlsx)} arquivos para processar.")
    
    # Processar arquivos
    todos_dataframes = []
    for arquivo in arquivos_xlsx:
        try:
            print(f"Processando arquivo: {os.path.basename(arquivo)}")
            resultado = extrair_dados_xlsx(arquivo)
            todos_dataframes.append(resultado['dados_tabela'])
        except Exception as e:
            print(f"Erro ao processar o arquivo {os.path.basename(arquivo)}: {str(e)}")
    
    if not todos_dataframes:
        print("Nenhum dado extraído dos arquivos.")
        return None
    
    # Concatenar e organizar colunas
    df_combinado = pl.concat(todos_dataframes)
    coluna_b = df_combinado.columns[0]
    df_final = df_combinado.rename({coluna_b: "NATUREZA"})
    
    # Reorganizar colunas
    colunas_ordenadas = ["NATUREZA", "CIDADE"] + [col for col in df_final.columns if col not in ["NATUREZA", "CIDADE"]]
    df_final = df_final.select(colunas_ordenadas)
    
    return df_final

# Caminhos e execução
pasta_dados = '/workspaces/SSPDF-Dash/Dados'
df_combinado = processar_todos_arquivos(pasta_dados)

if df_combinado is not None:
    caminho_saida = os.path.join('/workspaces/SSPDF-Dash', 'dados_combinados.csv')
    df_combinado.write_csv(caminho_saida)
    print(f"Dados combinados salvos em: {caminho_saida}")
    print(f"Total de linhas no arquivo combinado: {df_combinado.height}")
    print(f"Colunas no arquivo combinado: {df_combinado.columns}")
else:
    print("Não foi possível gerar o arquivo combinado.")