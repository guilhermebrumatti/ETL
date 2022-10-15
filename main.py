import pandas as pd

def read_files(path, name_file, year_date, type_file):
    _file = f'{path}{name_file}{year_date}.{type_file}'
    
    #lista com as posições das informações, segundo SeriesHistoricas_Layout.pdf disponibilizado no site do B3
    colspecs = [(2,10),
                (10,12),
                (12,24),
                (27,39),
                (56,69),
                (69,82),
                (82,95),
                (108,121),
                (152,170),
                (170,188)            

    ]
    
    #lista com os nomes das colunas
    names = ['data_pregao', 'codbdi', 'sigla_acao','nome_acao','preco_abertura','preco_maximo','preco_minimo','preco_fechamento','qtd_negocios','volume_negocios']

    df = pd.read_fwf(_file, colspecs = colspecs, names = names, skiprows =1)
    return df

#filtrando a tabela pela coluna codbdi, para ficar apenas resultados com número 2
def filter_stocks(df):
    df = df [df['codbdi']== 2]
    del df['codbdi']
    return df

#tratando coluna data_pregao, para formato mais amigavel
def parse_date(df):
    df['data_pregao'] = pd.to_datetime(df['data_pregao'], format ='%Y%m%d')
    return df

#transformando os valores em float com apenas duas casas decimais
def parse_values(df):
    df['preco_abertura'] = (df['preco_abertura'] /100).astype(float)
    df['preco_maximo'] = (df['preco_maximo'] /100).astype(float)
    df['preco_minimo'] = (df['preco_minimo'] /100).astype(float)
    df['preco_fechamento'] = (df['preco_fechamento'] /100).astype(float)
    return df

#agrupando arquivos
def concat_files(path, name_file, year_date, type_file, final_file):
    
    for i , y in enumerate(year_date):
        df = read_files(path, name_file, y, type_file)
        df = filter_stocks(df)
        df = parse_date(df)
        df = parse_values(df)
        
        if i==0:
            df_final = df
        else:
            df_final = pd.concat([df_final, df])
    #exportando dataframe final em formato .CSV
    df_final.to_csv(f'{path}//{final_file}', index=False)

#executando script ETL
year_date = ['2021','2022']
path =''
name_file='COTAHIST_A'
type_file ='TXT'
final_file = 'TabelaFinal.csv'
concat_files( path, name_file, year_date,type_file, final_file)