import streamlit as st
import pandas as pd
import mysql.connector
import decimal
import gspread
from google.oauth2 import service_account
from babel.numbers import format_currency

def puxar_aba_simples(id_gsheet, nome_aba, nome_df):

    nome_credencial = st.secrets["CREDENCIAL_SHEETS"]
    credentials = service_account.Credentials.from_service_account_info(nome_credencial)
    scope = ['https://www.googleapis.com/auth/spreadsheets']
    credentials = credentials.with_scopes(scope)
    client = gspread.authorize(credentials)

    spreadsheet = client.open_by_key(id_gsheet)
    
    sheet = spreadsheet.worksheet(nome_aba)

    sheet_data = sheet.get_all_values()

    st.session_state[nome_df] = pd.DataFrame(sheet_data[1:], columns=sheet_data[0])

def tratar_colunas_numero_df(df, lista_colunas):

    for coluna in lista_colunas:

        df[coluna] = (df[coluna].str.replace('.', '', regex=False).str.replace(',', '.', regex=False))

        df[coluna] = pd.to_numeric(df[coluna])

def tratar_colunas_data_df(df, lista_colunas):

    for coluna in lista_colunas:

        df[coluna] = pd.to_datetime(df[coluna], format='%d/%m/%Y').dt.date

def gerar_df_metas():

    puxar_aba_simples(st.session_state.id_gsheet_metas_vendas, 'BD - Metas', 'df_metas')

    tratar_colunas_numero_df(st.session_state.df_metas, st.session_state.lista_colunas_numero_df_metas)

    tratar_colunas_data_df(st.session_state.df_metas, st.session_state.lista_colunas_data_df_metas)

    st.session_state.df_metas['Mes_Ano'] = pd.to_datetime(st.session_state.df_metas['Data']).dt.to_period('M')

    st.session_state.df_metas['Meta_Total'] = st.session_state.df_metas['Meta_Guia'] + st.session_state.df_metas['Meta_PDV'] + st.session_state.df_metas['Meta_HV'] + \
        st.session_state.df_metas['Meta_Grupos'] + st.session_state.df_metas['Meta_VendasOnline']

def gerar_df_phoenix(base_luck, request_select):
    
    config = {'user': 'user_automation_jpa', 'password': 'luck_jpa_2024', 'host': 'comeia.cixat7j68g0n.us-east-1.rds.amazonaws.com', 'database': base_luck}

    conexao = mysql.connector.connect(**config)

    cursor = conexao.cursor()

    request_name = request_select

    cursor.execute(request_name)

    resultado = cursor.fetchall()
    
    cabecalho = [desc[0] for desc in cursor.description]

    cursor.close()

    conexao.close()

    df = pd.DataFrame(resultado, columns=cabecalho)

    df = df.applymap(lambda x: float(x) if isinstance(x, decimal.Decimal) else x)

    return df

def gerar_df_vendas_final():

    def gerar_df_vendas_phoenix():

        request_select = '''SELECT Canal_de_Vendas, Vendedor, Nome_Segundo_Vendedor, Status_Financeiro, Data_Venda, Valor_Venda, Nome_Estabelecimento_Origem, Desconto_Global_Por_Servico, 
        Desconto_Global, Nome_Parceiro, Cod_Reserva, Nome_Servico, `Total ADT`, `Total CHD` 
        FROM vw_sales
        WHERE Status_Financeiro NOT IN ('TROCADO', 'A Faturar')'''

        st.session_state.df_vendas = gerar_df_phoenix(st.session_state.base_luck, request_select)

    def gerar_df_vendas_manuais():

        puxar_aba_simples(st.session_state.id_gsheet_metas_vendas, 'BD - Vendas Manuais', 'df_vendas_manuais')

        tratar_colunas_numero_df(st.session_state.df_vendas_manuais, st.session_state.lista_colunas_numero_df_vendas_manuais)

        tratar_colunas_data_df(st.session_state.df_vendas_manuais, st.session_state.lista_colunas_data_df_vendas_manuais)

    def ajustar_nomes_leticia_soraya(df_vendas):

        df_vendas['Vendedor'] = df_vendas['Vendedor'].replace('SORAYA - TRANSFERISTA', 'SORAYA - GUIA')

        df_vendas.loc[(df_vendas['Vendedor']=='LETICIA - TRANSFERISTA') & (pd.to_datetime(df_vendas['Data_Venda']).dt.year>=2025), 'Vendedor'] = 'LETICIA - GUIA'

        df_vendas.loc[(df_vendas['Vendedor']=='LETICIA - TRANSFERISTA') & (pd.to_datetime(df_vendas['Data_Venda']).dt.year<2025), 'Vendedor'] = 'LETICIA - PDV'

        return df_vendas

    def ajustar_pdvs_facebook(df_vendas):

        mask_ref = (df_vendas['Vendedor'].isin(['RAQUEL - PDV', 'VALERIA - PDV', 'ROBERTA - PDV', 'LETICIA - PDV'])) & (pd.to_datetime(df_vendas['Data_Venda']).dt.year<2025) & \
                (df_vendas['Canal_de_Vendas']=='Facebook')

        df_vendas.loc[mask_ref, 'Vendedor'] = df_vendas.loc[mask_ref, 'Vendedor'].apply(lambda x: x.replace('- PDV', '- GUIA'))

        return df_vendas

    def ajustar_colunas_data_venda_mes_ano_total_paxs(df_vendas):

        df_vendas['Data_Venda'] = pd.to_datetime(df_vendas['Data_Venda']).dt.date

        df_vendas['Ano'] = pd.to_datetime(df_vendas['Data_Venda']).dt.year

        df_vendas['Mes'] = pd.to_datetime(df_vendas['Data_Venda']).dt.month

        df_vendas['Mes_Ano'] = pd.to_datetime(df_vendas['Data_Venda']).dt.to_period('M')

        df_vendas['Total Paxs'] = df_vendas['Total ADT'].fillna(0) + df_vendas['Total CHD'].fillna(0) / 2

        return df_vendas
    
    def criar_coluna_setor_definir_metas(df_vendas):

        df_vendas['Setor'] = df_vendas['Vendedor'].str.split(' - ').str[1].replace({'OPERACIONAL':'LOGISTICA', 'BASE AEROPORTO ': 'LOGISTICA', 'BASE AEROPORTO': 'LOGISTICA', 
                                                                                    'COORD. ESCALA': 'LOGISTICA', 'KUARA/MANSEAR': 'LOGISTICA', 'MOTORISTA': 'LOGISTICA', 
                                                                                    'SUP. LOGISTICA': 'LOGISTICA'})
        
        dict_setor_meta = {'GUIA': 'Meta_Guia', 'PDV': 'Meta_PDV', 'HOTEL VENDAS': 'Meta_HV', 'GRUPOS': 'Meta_Grupos', 'VENDAS ONLINE': 'Meta_VendasOnline'}

        df_metas_indexed = st.session_state.df_metas.set_index('Mes_Ano')

        df_vendas['Meta'] = df_vendas.apply(lambda row: df_metas_indexed.at[row['Mes_Ano'], dict_setor_meta[row['Setor']]] 
                                            if row['Setor'] in dict_setor_meta and row['Mes_Ano'] in df_metas_indexed.index else 0, axis=1)
        
        return df_vendas

    # Puxando as vendas do Phoenix

    gerar_df_vendas_phoenix()

    # Puxando as vendas lançadas manualmente na planilha

    gerar_df_vendas_manuais()

    df_vendas = pd.concat([st.session_state.df_vendas, st.session_state.df_vendas_manuais], ignore_index=True)

    # Ajustando nomes de letícia e soraya pra identificar o setor correto

    df_vendas = ajustar_nomes_leticia_soraya(df_vendas)

    # Identificando como guia Raquel, Valeria, Roberta e Letícia quando o canal de vendas é Facebook e o ano é antes de 2025

    df_vendas = ajustar_pdvs_facebook(df_vendas)

    # Ajustando formato de Data_Venda, criando coluna Mes_Ano e criando coluna Total Paxs

    df_vendas = ajustar_colunas_data_venda_mes_ano_total_paxs(df_vendas)

    # Criando coluna setor, identificando pessoal da logistica e colocando a meta p/ cada setor

    df_vendas = criar_coluna_setor_definir_metas(df_vendas)

    return df_vendas

def gerar_df_descontos():

    df_descontos = st.session_state.df_vendas_final

    df_descontos['Mes_Nome'] = df_descontos['Mes_Ano'].dt.strftime('%B')

    df_descontos = df_descontos[df_descontos['Setor'].isin(st.session_state.setores_desejados_gerencial)]

    return df_descontos

def colher_selecao_ano_mes_vendedor(df_descontos):

    lista_anos = df_descontos['Ano'].unique().tolist()

    lista_mes = [valor for valor in st.session_state.meses_disponiveis.keys() if valor in df_descontos['Mes_Nome'].unique()]

    lista_vendedor = sorted(df_descontos['Vendedor'].unique().tolist())

    seleciona_ano = st.multiselect('Selecione o Ano', options=lista_anos, default=[], key='reemb_0001')

    seleciona_mes = st.multiselect('Selecione o Mes', options=lista_mes, default=[], key='reemb_0002')

    seleciona_vendedor = st.multiselect('Selecione o Vendedor', options=lista_vendedor, default=[], key='reemb_0003')

    return seleciona_ano, seleciona_mes, seleciona_vendedor

def formatar_moeda(valor):

    return format_currency(valor, 'BRL', locale='pt_BR')

def gerar_df_agrupado_descontos(df_descontos, seleciona_ano, seleciona_mes, seleciona_vendedor):

    df_descontos1 = df_descontos[(df_descontos['Ano'].isin(seleciona_ano)) & (df_descontos['Mes_Nome'].isin(seleciona_mes)) & (df_descontos['Vendedor'].isin(seleciona_vendedor))]

    df_descontos1.loc[df_descontos1['Nome_Servico']=='EXTRA', 'Desconto_Global'] = 0

    df_agrupado_descontos = df_descontos1.groupby(['Cod_Reserva', 'Vendedor']).agg({'Mes_Ano': 'first', 'Desconto_Global': 'min', 'Valor_Venda': 'sum', 'Nome_Servico': 'count', 'Total Paxs': 'max', 
                                                                                    'Setor': 'first'}).reset_index()

    df_agrupado_descontos['Valor_Servico'] = df_agrupado_descontos['Valor_Venda'] + df_agrupado_descontos['Desconto_Global']

    df_agrupado_descontos['% Desconto'] = df_agrupado_descontos.apply(lambda row: f"{round((row['Desconto_Global'] / row['Valor_Servico']) * 100, 2)}%", axis=1)

    df_agrupado_descontos['TM Reserva'] = df_agrupado_descontos['Valor_Venda'] / df_agrupado_descontos['Total Paxs']

    df_agrupado_descontos['Desconto_Global'] = df_agrupado_descontos['Desconto_Global'].fillna(0)

    return df_agrupado_descontos

def gerar_df_filtrado_print(df_agrupado_descontos):

    df_filtrado_print = df_agrupado_descontos.copy()

    df_filtrado_print['Valor_Venda'] = df_filtrado_print['Valor_Venda'].apply(formatar_moeda)

    df_filtrado_print['Desconto_Global'] = df_filtrado_print['Desconto_Global'].apply(formatar_moeda)

    df_filtrado_print['TM Reserva'] = df_filtrado_print['TM Reserva'].apply(formatar_moeda)

    return df_filtrado_print

def gerar_df_individual(df_agrupado_descontos):

    df_individual = df_agrupado_descontos.groupby(['Vendedor']).agg({'Valor_Venda': 'sum', 'Valor_Servico': 'sum', 'Total Paxs': 'sum', 'Desconto_Global': 'sum',}).reset_index()

    df_individual['% Desconto'] = df_individual.apply(lambda row: f"{round((row['Desconto_Global'] / row['Valor_Servico']) * 100, 2)}%", axis=1)

    return df_individual

st.set_page_config(layout='wide')

st.title('Descontos')

st.write(f'_Total de Vendas Considerando apenas as Vendas Realizadas - Não considerando os reembolsos_')

st.divider()

if any(key not in st.session_state for key in ['df_config', 'df_metas', 'df_vendas_final', 'df_paxs_in']):

    with st.spinner('Puxando reembolsos, configurações, histórico...'):

        puxar_aba_simples(st.session_state.id_gsheet_metas_vendas, 'Configurações Vendas', 'df_config')

        gerar_df_metas()

    with st.spinner('Puxando vendas, ranking, guias IN e paxs IN do Phoenix...'):

        st.session_state.df_vendas_final = gerar_df_vendas_final()

df_descontos = gerar_df_descontos()

seleciona_ano, seleciona_mes, seleciona_vendedor = colher_selecao_ano_mes_vendedor(df_descontos)

if len(seleciona_ano)>0 and len(seleciona_mes)>0 and len(seleciona_vendedor)>0:

    df_agrupado_descontos = gerar_df_agrupado_descontos(df_descontos, seleciona_ano, seleciona_mes, seleciona_vendedor)

    df_filtrado_print = gerar_df_filtrado_print(df_agrupado_descontos)

    st.dataframe(df_filtrado_print[['Cod_Reserva', 'Valor_Venda', 'Desconto_Global', '% Desconto', 'Nome_Servico', 'Total Paxs', 'TM Reserva']]
                .rename(columns={'Cod_Reserva': 'Reserva', 'Valor_Venda': 'Total Venda', 'Desconto_Global': 'Total Desconto', 'Nome_Servico': 'Passeios Vendidos'}), hide_index=True, 
                use_container_width=True)

    df_individual = gerar_df_individual(df_agrupado_descontos)

    st.dataframe(df_individual, hide_index=True, use_container_width=True)

else:

    st.warning('Seleciona pelo menos um Ano, Mês e Vendedor')
