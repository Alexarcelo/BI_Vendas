import streamlit as st
import pandas as pd
import mysql.connector
import decimal
import gspread
from google.oauth2 import service_account
import datetime
import numpy as np
from babel.numbers import format_currency
import plotly.express as px
import plotly.graph_objects as go

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

def gerar_df_reembolsos():

    puxar_aba_simples(st.session_state.id_gsheet_reembolsos, 'BD - Geral', 'df_reembolsos')

    tratar_colunas_numero_df(st.session_state.df_reembolsos, st.session_state.lista_colunas_numero_df_reembolsos)

    tratar_colunas_data_df(st.session_state.df_reembolsos, st.session_state.lista_colunas_data_df_reembolsos)

    st.session_state.df_reembolsos['Ano'] = pd.to_datetime(st.session_state.df_reembolsos['Data_venc']).dt.year
    
    st.session_state.df_reembolsos['Mes'] = pd.to_datetime(st.session_state.df_reembolsos['Data_venc']).dt.month

    st.session_state.df_reembolsos['Mes_Ano'] = pd.to_datetime(st.session_state.df_reembolsos['Data_venc']).dt.to_period('M')

def gerar_df_metas_vendedor():

    puxar_aba_simples(st.session_state.id_gsheet_metas_vendas, 'BD - Metas_Vendedor', 'df_metas_vendedor')

    tratar_colunas_numero_df(st.session_state.df_metas_vendedor, st.session_state.lista_colunas_numero_df_metas_vendedor)

    tratar_colunas_data_df(st.session_state.df_metas_vendedor, st.session_state.lista_colunas_data_df_metas_vendedor)

    st.session_state.df_metas_vendedor['Mes_Ano'] = pd.to_datetime(st.session_state.df_metas_vendedor['Data']).dt.to_period('M')

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

def gerar_df_ranking():

    request_select = '''SELECT `1 Vendedor`, `Data de Execucao`, `Tipo de Servico`, `Servico`, `Total ADT`, `Total CHD`, `Codigo da Reserva` 
    FROM vw_sales_ranking 
    WHERE `Tipo de Servico` = 'TOUR';'''
    
    st.session_state.df_ranking = gerar_df_phoenix(st.session_state.base_luck, request_select)

    st.session_state.df_ranking['Data de Execucao'] = pd.to_datetime(st.session_state.df_ranking['Data de Execucao']).dt.date

    st.session_state.df_ranking['Ano'] = pd.to_datetime(st.session_state.df_ranking['Data de Execucao']).dt.year
    
    st.session_state.df_ranking['Mes'] = pd.to_datetime(st.session_state.df_ranking['Data de Execucao']).dt.month
    
    st.session_state.df_ranking['Mes_Ano'] = pd.to_datetime(st.session_state.df_ranking['Data de Execucao']).dt.to_period('M')

    st.session_state.df_ranking['Setor'] = st.session_state.df_ranking['1 Vendedor'].str.split(' - ').str[1].replace({'OPERACIONAL':'LOGISTICA', 'BASE AEROPORTO ': 'LOGISTICA', 
                                                                                                                      'BASE AEROPORTO': 'LOGISTICA', 'COORD. ESCALA': 'LOGISTICA', 
                                                                                                                      'KUARA/MANSEAR': 'LOGISTICA'})
    
    st.session_state.df_ranking['Total Paxs'] = st.session_state.df_ranking['Total ADT'] + st.session_state.df_ranking['Total CHD'] / 2

def gerar_df_guias_in():

    request_select = '''SELECT `Data da Escala`, `Guia`, `Tipo de Servico`, `Total ADT`, `Total CHD` 
    FROM vw_payment_guide 
    WHERE `Tipo de Servico` = 'IN';'''
    
    st.session_state.df_guias_in = gerar_df_phoenix(st.session_state.base_luck, request_select)

    st.session_state.df_guias_in['Total_Paxs'] = st.session_state.df_guias_in['Total ADT'].fillna(0) + (st.session_state.df_guias_in['Total CHD'].fillna(0) / 2)

    st.session_state.df_guias_in['Data da Escala'] = pd.to_datetime(st.session_state.df_guias_in['Data da Escala']).dt.date

    substituicao = {'RAQUEL - PDV': 'RAQUEL - GUIA', 'VALERIA - PDV': 'VALERIA - GUIA', 'ROBERTA - PDV': 'ROBERTA - GUIA', 'LETICIA - TRANSFERISTA': 'LETICIA - GUIA', 
                    'SORAYA - BASE AEROPORTO ': 'SORAYA - GUIA', 'SORAYA - TRANSFERISTA': 'SORAYA - GUIA'}

    st.session_state.df_guias_in['Guia'] = st.session_state.df_guias_in['Guia'].replace(substituicao)

    st.session_state.df_guias_in['Mes_Ano'] = pd.to_datetime(st.session_state.df_guias_in['Data da Escala']).dt.to_period('M')

def gerar_df_paxs_in():

    request_select = '''SELECT Reserva, Parceiro, `Tipo de Servico`, `Data Execucao`, `Servico`, `Status do Servico`, `Total ADT`, `Total CHD` 
    FROM vw_router 
    WHERE `Servico` NOT IN ('GUIA BASE NOTURNO', 'AEROPORTO JOÃO PESSOA / HOTÉIS PITIMBU', 'AEROPORTO JOÃO PESSOA / HOTÉIS CAMPINA GRANDE', 'FAZER CONTATO - SEM TRF IN', 
    'AEROPORTO CAMPINA GRANDE / HOTEL CAMPINA GRANDE', 'GUIA BASE DIURNO') AND `Tipo de Servico` = 'IN' AND `Status do Servico` != 'CANCELADO';'''
    
    st.session_state.df_paxs_in = gerar_df_phoenix(st.session_state.base_luck, request_select)

    st.session_state.df_paxs_in['Data Execucao'] = pd.to_datetime(st.session_state.df_paxs_in['Data Execucao']).dt.date
    
    st.session_state.df_paxs_in['Ano'] = pd.to_datetime(st.session_state.df_paxs_in['Data Execucao']).dt.year
    
    st.session_state.df_paxs_in['Mes'] = pd.to_datetime(st.session_state.df_paxs_in['Data Execucao']).dt.month
    
    st.session_state.df_paxs_in['Mes_Ano'] = pd.to_datetime(st.session_state.df_paxs_in['Data Execucao']).dt.to_period('M')

    st.session_state.df_paxs_in['Total_Paxs'] = st.session_state.df_paxs_in['Total ADT'] + (st.session_state.df_paxs_in['Total CHD'] / 2)

    st.session_state.df_paxs_in = pd.merge(st.session_state.df_paxs_in, st.session_state.df_metas[['Mes_Ano', 'Paxs_Desc']], on='Mes_Ano', how='left')

def gerar_lista_setor():

    lista_setor = sorted(st.session_state.df_vendas_final['Setor'].str.strip().dropna().unique().tolist())

    lista_setor.insert(0, '--- Todos ---')

    lista_setor = [item for item in lista_setor if item not in ['COORD. FINANCEIRO', 'COORD. VENDAS', 'LOGISTICA', 'SAC', 'GUIA TOUR AZUL', 'PLANEJAMENTO ESTRATÉGICO', 'COMERCIAL', 'DIRETORA', 
                                                                'SUP. EXPERIÊNCIA AO CLIENTE/SAC']]

    return lista_setor

def filtrar_periodo_dfs():

    df_vendas = st.session_state.df_vendas_final[(st.session_state.df_vendas_final['Data_Venda'] >= data_ini) & (st.session_state.df_vendas_final['Data_Venda'] <= data_fim)].reset_index(drop=True)

    df_paxs_in = st.session_state.df_paxs_in[(st.session_state.df_paxs_in['Data Execucao'] >= data_ini) & (st.session_state.df_paxs_in['Data Execucao'] <= data_fim)].reset_index(drop=True)

    df_metas_vendedor = st.session_state.df_metas_vendedor[(st.session_state.df_metas_vendedor['Data'] >= data_ini) & (st.session_state.df_metas_vendedor['Data'] <= data_fim)]\
        .reset_index(drop=True)
    
    df_reembolsos = st.session_state.df_reembolsos[(st.session_state.df_reembolsos['Data_venc'] >= data_ini) & (st.session_state.df_reembolsos['Data_venc'] <= data_fim)].reset_index(drop=True)

    df_guias_in = st.session_state.df_guias_in[(st.session_state.df_guias_in['Data da Escala'] >= data_ini) & (st.session_state.df_guias_in['Data da Escala'] <= data_fim)]\
        .reset_index(drop=True)
        
    df_metas_setor = st.session_state.df_metas[(st.session_state.df_metas['Data'] >= data_ini) & (st.session_state.df_metas['Data'] <= data_fim)].reset_index(drop=True)

    return df_vendas, df_paxs_in, df_metas_vendedor, df_reembolsos, df_guias_in, df_metas_setor

def criar_listas_vendedor_canal_hotel(df_vendas):

    lista_vendedor = sorted(df_vendas['Vendedor'].dropna().unique().tolist())

    lista_vendedor.insert(0, '--- Todos ---')

    lista_canal = sorted(df_vendas['Canal_de_Vendas'].dropna().unique().tolist())

    lista_canal.insert(0, '--- Todos ---')

    lista_hotel = sorted(df_vendas['Nome_Estabelecimento_Origem'].dropna().unique().tolist())

    lista_hotel.insert(0, '--- Todos ---')

    return lista_vendedor, lista_canal, lista_hotel

def filtrar_canal_vendedor_hotel_df_vendas(df_vendas, seleciona_canal, seleciona_vend, seleciona_hotel):
    
    if len(seleciona_canal)>0 and '--- Todos ---' not in seleciona_canal:

        df_vendas = df_vendas[df_vendas['Canal_de_Vendas'].isin(seleciona_canal)]

    if len(seleciona_vend)>0 and '--- Todos ---' not in seleciona_vend:

        df_vendas = df_vendas[df_vendas['Vendedor'].isin(seleciona_vend)]
    
    if len(seleciona_hotel)>0 and '--- Todos ---' not in seleciona_hotel:

        df_vendas = df_vendas[df_vendas['Nome_Estabelecimento_Origem'].isin(seleciona_hotel)]

    return df_vendas

def gerar_df_hotel(df_vendas):

    df_hotel = df_vendas.copy()

    df_hotel.rename(columns={'Nome_Estabelecimento_Origem': 'Hotel', 'Desconto_Global_Por_Servico': 'Desconto Reserva x Serviços'}, inplace=True)

    df_hotel = df_hotel.groupby(['Mes_Ano','Vendedor', 'Hotel']).agg({'Valor_Venda': 'sum', 'Desconto Reserva x Serviços': 'sum'}).reset_index()

    return df_hotel

def ajustar_desconto_global(df_vendas):

    valor_ref = np.where(df_vendas['Data_Venda'] >= datetime.date(2024, 12, 1), 1000, 5000)

    df_vendas['Desconto_Global_Ajustado'] = np.where((df_vendas['Desconto_Global_Por_Servico'].notna()) & (df_vendas['Desconto_Global_Por_Servico'] < valor_ref) & 
                                                     (df_vendas['Nome_Servico'] != 'EXTRA'), df_vendas['Desconto_Global_Por_Servico'], 0)
    
    return df_vendas

def gerar_df_vendas_agrupado(df_vendas, df_reembolsos, df_metas_vendedor, df_guias_in, df_paxs_in, df_metas_setor):

    def inserindo_paxs_in_vendedor(df_vendas_agrupado, df_guias_in):

        df_guias_in = df_guias_in.rename(columns={'Total_Paxs': 'Paxs_IN', 'Guia': 'Vendedor'})

        df_vendas_agrupado = pd.merge(df_vendas_agrupado, df_guias_in[['Vendedor', 'Paxs_IN']], on='Vendedor', how='left')

        return df_vendas_agrupado

    def calculando_soma_total_paxs_paxs_desc(df_paxs_in, df_metas_setor, df_vendas_agrupado):

        total_paxs_in = df_paxs_in['Total_Paxs'].sum()

        total_paxs_desc = df_metas_setor['Paxs_Desc'].sum()

        df_vendas_agrupado['Total_Paxs'] = total_paxs_in + total_paxs_desc

        return df_vendas_agrupado

    def calculando_valor_venda_liquida_reembolso(df_vendas_agrupado, df_reembolsos):

        df_vendas_agrupado = pd.merge(df_vendas_agrupado, df_reembolsos, on='Vendedor', how='left')

        df_vendas_agrupado['Venda_Filtrada'] = df_vendas_agrupado['Valor_Venda'].fillna(0) - df_vendas_agrupado['Valor_Total'].fillna(0)

        return df_vendas_agrupado

    def inserindo_meta_vendedor_periodo(df_metas_vendedor, df_vendas_agrupado):

        df_metas_vendedor_periodo = df_metas_vendedor.groupby('Vendedor', as_index=False)['Meta_Mes'].mean()

        df_vendas_agrupado = pd.merge(df_vendas_agrupado, df_metas_vendedor_periodo, on='Vendedor', how='left')

        return df_vendas_agrupado

    df_vendas_agrupado = df_vendas.groupby(['Vendedor', 'Setor'], dropna=False).agg({'Valor_Venda': 'sum', 'Desconto_Global_Ajustado': 'sum', 'Meta': 'mean', 'Nome_Servico': 'count', 
                                                                                     'Cod_Reserva': 'nunique'}).reset_index()
    
    df_vendas_agrupado = inserindo_paxs_in_vendedor(df_vendas_agrupado, df_guias_in)

    df_vendas_agrupado = calculando_soma_total_paxs_paxs_desc(df_paxs_in, df_metas_setor, df_vendas_agrupado)

    df_vendas_agrupado = calculando_valor_venda_liquida_reembolso(df_vendas_agrupado, df_reembolsos)

    df_vendas_agrupado['Venda_por_Reserva'] = df_vendas_agrupado['Nome_Servico'] / df_vendas_agrupado['Cod_Reserva']

    df_vendas_agrupado['Ticket_Medio'] = np.where(df_vendas_agrupado['Setor'] == 'GUIA', df_vendas_agrupado['Venda_Filtrada'] / df_vendas_agrupado['Paxs_IN'], 
                                                  df_vendas_agrupado['Venda_Filtrada'] / df_vendas_agrupado['Total_Paxs'])
    
    df_vendas_agrupado['Ticket_Medio'] = df_vendas_agrupado['Ticket_Medio'].fillna(0)
    
    df_vendas_agrupado = df_vendas_agrupado.sort_values(by='Venda_Filtrada', ascending=False)

    df_vendas_agrupado = inserindo_meta_vendedor_periodo(df_metas_vendedor, df_vendas_agrupado)

    return df_vendas_agrupado

def formatar_moeda(valor):

    return format_currency(valor, 'BRL', locale='pt_BR')

def gerar_soma_vendas_tm_vendas_desconto_paxs_recebidos(df_vendas_agrupado, df_metas_setor):

    def gerar_media_descontos(total_desconto, soma_vendas):

        total_desconto, soma_vendas = float(total_desconto), float(soma_vendas)

        if total_desconto == 0 and soma_vendas == 0:

            return '0%'
        
        med_desconto = (total_desconto / (soma_vendas + total_desconto)) * 100

        return f'{round(med_desconto, 2)}%'
    
    df_vendas_setores_desejados = df_vendas_agrupado[df_vendas_agrupado['Setor'].isin(st.session_state.setores_desejados_gerencial)]
        
    soma_vendas = df_vendas_setores_desejados['Venda_Filtrada'].sum()

    tm_vendas = soma_vendas / df_vendas_setores_desejados['Total_Paxs'].mean()

    if (st.session_state.seleciona_setor[0]=='--- Todos ---' and len(st.session_state.seleciona_setor)==1) or \
    ('--- Todos ---' not in st.session_state.seleciona_setor and len(st.session_state.seleciona_setor)==6):
        
        tm_setor_estip = df_metas_setor['Meta_Total'].mean()

    else:

        df_setor_meta = df_vendas_setores_desejados.groupby('Setor', as_index=False)['Meta'].first()

        tm_setor_estip = df_setor_meta['Meta'].sum()

    total_desconto = df_vendas_setores_desejados[df_vendas_setores_desejados['Nome_Servico'] != 'EXTRA']['Desconto_Global_Ajustado'].sum()

    paxs_recebidos = int(df_vendas_setores_desejados['Total_Paxs'].mean())

    med_desconto = gerar_media_descontos(total_desconto, soma_vendas)

    return soma_vendas, tm_vendas, tm_setor_estip, total_desconto, paxs_recebidos, med_desconto

def gerar_meta_esperada_perc_alcancado(soma_vendas, tm_setor_estip, paxs_recebidos):

    meta_esperada_total = tm_setor_estip*paxs_recebidos

    meta_esperada_formatada = formatar_moeda(meta_esperada_total)

    perc_alcancado = f'{round((soma_vendas / meta_esperada_total) * 100, 2)}%'

    return meta_esperada_formatada, perc_alcancado

def plotar_quadrados_html(titulo, info_numero):
    
    st.markdown(f"""
    <div style="background-color:#f0f0f5; border-radius:10px; border: 2px solid #ccc; text-align: center; width: 180px; margin:0 auto; margin: 0 auto 10px auto; min-height: 50px;">
        <h3 style="color: #333; font-size: 18px; padding: 0px 10px; text-align: center; margin-bottom: 0px; ">{titulo}</h3>
        <h2 style="color: #047c6c; font-size: 20px; padding: 10px 30px; text-align: center; margin:0 auto; white-space: nowrap;">{info_numero}</h2>
    </div>
    """, unsafe_allow_html=True)

def gerar_df_estilizado(df_vendas_agrupado):
    
    def highlight_ticket(row):

        if row['Ticket Médio'] > row['Meta T.M.'] and row['Meta T.M.']>0:

            return ['background-color: lightgreen'] * len(row)
        
        else:

            return [''] * len(row)
    
    df_estilizado = df_vendas_agrupado[['Vendedor', 'Venda_Filtrada', 'Ticket_Medio', 'Meta_Mes', 'Venda_por_Reserva', 'Desconto_Global_Ajustado']].copy()

    df_estilizado['% Desconto'] = (df_estilizado['Desconto_Global_Ajustado'] / (df_estilizado['Venda_Filtrada'] + df_estilizado['Desconto_Global_Ajustado']))*100

    df_estilizado['% Desconto'] = df_estilizado['% Desconto'].fillna(0)

    df_estilizado['Meta_Mes'] = df_estilizado['Meta_Mes'].replace(0, None).fillna(df_vendas_agrupado['Meta'])

    df_estilizado = df_estilizado.rename(columns={'Venda_por_Reserva': 'Venda_Reser', 'Desconto_Global_Ajustado': 'Total_Descontos', 'Venda_Filtrada': 'Valor_Vendas'})

    df_estilizado = df_estilizado.drop_duplicates(keep='last')

    df_estilizado.columns = ['Vendedor', 'Vendas', 'Ticket Médio', 'Meta T.M.', 'Venda por Reserva', 'R$ Descontos', '% Descontos']

    df_estilizado = df_estilizado.style.apply(highlight_ticket, axis=1)

    df_estilizado = df_estilizado.format({'Vendas': formatar_moeda, 'Ticket Médio': formatar_moeda, 'Meta T.M.': formatar_moeda, 'Venda por Reserva': '{:.2f}'.format, 
                                          'R$ Descontos': formatar_moeda, '% Descontos':'{:.2f}%'.format})
    
    return df_estilizado

def gerar_grafico_todos_setores(df_setor_agrupado):

    fig = px.bar(x=df_setor_agrupado['Setor'],  y=df_setor_agrupado['Venda_Filtrada'], color=df_setor_agrupado['Setor'], title='Valor Total por Setor', 
                 labels={'Venda_Filtrada': 'Valor Total', 'Setor': 'Setores'}, text=df_setor_agrupado['Venda_Filtrada'].apply(formatar_moeda))
    
    fig.update_traces(textposition='outside', textfont=dict(size=10))

    fig.update_layout(yaxis_title='Valor Total', xaxis_title='Setores')

    return fig

def gerar_grafico_setor_especifico(df_vendas_agrupado):

    max_venda = df_vendas_agrupado['Venda_Filtrada'].max() * 2

    max_tm = df_vendas_agrupado['Ticket_Medio'].max()*1.1

    fig = px.bar(x=df_vendas_agrupado['Vendedor'], y=df_vendas_agrupado['Venda_Filtrada'], color=df_vendas_agrupado['Vendedor'], title='Valor Total por Vendedor', 
                 labels={'Venda_Filtrada': 'Valor Total', 'Vendedor': 'Vendedores'}, text=df_vendas_agrupado['Venda_Filtrada'].apply(formatar_moeda))
    
    fig.update_traces(textposition='outside', textfont=dict(size=10))

    fig.update_layout(yaxis_title='Valor Total', xaxis_title='Vendedores', yaxis=dict(range=[0, max_venda]), 
                      yaxis2=dict( title="Ticket Médio", overlaying="y", side="right", showgrid=False, range=[0, max_tm]))

    fig.add_trace(go.Scatter(x=df_vendas_agrupado['Vendedor'], y=df_vendas_agrupado['Ticket_Medio'], mode='lines+markers+text', name='Ticket Médio', line=dict(width=1), marker=dict(size=4), 
                             yaxis='y2', line_shape='spline', text=df_vendas_agrupado['Ticket_Medio'].apply(formatar_moeda), textposition='top center', textfont=dict(size=10)))

    return fig

def gerar_df_todos_vendedores_filtrado(df_cont_passeio, passeios_incluidos):

    df_todos_vendedores_filtrado = df_cont_passeio[df_cont_passeio['Nome_Servico'].isin(passeios_incluidos)]

    df_todos_vendedores_filtrado['Nome_Servico'] = df_todos_vendedores_filtrado['Nome_Servico'].replace({'EMBARCAÇAO - CATAMARÃ DO FORRÓ ': 'CATAMARÃ DO FORRÓ', 
                                                                                                         'INGRESSO - BY NIGHT ': 'BY NIGHT PARAHYBA OXENTE '}) 

    return df_todos_vendedores_filtrado

def gerar_grafico_pizza_todos_vendedores(df_todos_vendedores_filtrado, passeios_incluidos):

    fig = px.pie(df_todos_vendedores_filtrado, names='Nome_Servico', values='Total Paxs', title='Distribuição de Paxs por Passeio', category_orders={'Nome_Servico': passeios_incluidos})

    fig.update_traces(texttemplate='%{percent}', hovertemplate='%{label}: %{value} Paxs')

    fig.update_layout(showlegend=True, margin=dict(t=50, b=50, l=50, r=50))

    return fig

def gerar_df_vendedor_filtrado(df_cont_passeio, passeios_incluidos, vendedor):

    df_vendedor_filtrado = df_cont_passeio[(df_cont_passeio['Vendedor'] == vendedor) & (df_cont_passeio['Nome_Servico'].isin(passeios_incluidos))]

    df_vendedor_filtrado['Nome_Servico'] = df_vendedor_filtrado['Nome_Servico'].replace({'EMBARCAÇAO - CATAMARÃ DO FORRÓ ': 'CATAMARÃ DO FORRÓ', 'INGRESSO - BY NIGHT ': 'BY NIGHT PARAHYBA OXENTE '}) 

    return df_vendedor_filtrado

def gerar_grafico_pizza_vendedor(df_vendedor_filtrado, vendedor, passeios_incluidos):

    fig = px.pie(df_vendedor_filtrado, names='Nome_Servico', values='Total Paxs', title=f'Distribuição de Paxs por Passeio - {vendedor}', category_orders={'Nome_Servico': passeios_incluidos})

    fig.update_traces(texttemplate='%{percent}', hovertemplate='%{label}: %{value} Paxs')
    
    fig.update_layout(showlegend=True, margin=dict(t=50, b=50, l=50, r=50))

    return fig

st.set_page_config(layout='wide')

if not 'base_luck' in st.session_state:
    
    base_fonte = st.query_params["base_luck"]

    if base_fonte=='mcz':

        st.session_state.base_luck = 'test_phoenix_maceio'
        
    elif base_fonte=='rec':

        st.session_state.base_luck = 'test_phoenix_recife'

    elif base_fonte=='ssa':

        st.session_state.base_luck = 'test_phoenix_salvador'

    elif base_fonte=='aju':

        st.session_state.base_luck = 'test_phoenix_aracaju'

    elif base_fonte=='fen':

        st.session_state.base_luck = 'test_phoenix_noronha'

    elif base_fonte=='nat':

        st.session_state.base_luck = 'test_phoenix_natal'

    elif base_fonte=='jpa':

        st.session_state.base_luck = 'test_phoenix_joao_pessoa'

        st.session_state.id_gsheet_metas_vendas = '1rkHSZ8fGqcITG9GMPzWCdIsCW11aC51-HfxggTeeLZQ'
        
        st.session_state.lista_colunas_numero_df_vendas_manuais = ['Valor_Venda', 'Desconto_Global_Por_Servico', 'Total ADT', 'Total CHD']
        
        st.session_state.lista_colunas_data_df_vendas_manuais = ['Data_Venda']
        
        st.session_state.lista_colunas_numero_df_metas_vendedor = ['Meta_Mes']
        
        st.session_state.lista_colunas_data_df_metas_vendedor = ['Data']
        
        st.session_state.lista_colunas_numero_df_metas = ['Meta_Guia', 'Meta_PDV', 'Meta_HV', 'Meta_Grupos', 'Meta_VendasOnline', 'Paxs_Desc']
        
        st.session_state.lista_colunas_data_df_metas = ['Data']
        
        st.session_state.lista_colunas_numero_df_historico = ['Valor_Venda', 'Paxs ADT', 'Paxs CHD']
        
        st.session_state.lista_colunas_data_df_historico = ['Data']
        
        st.session_state.lista_colunas_numero_df_historico_vendedor = ['Valor', 'Meta', 'Paxs_Total']
        
        st.session_state.lista_colunas_data_df_historico_vendedor = ['Data']
        
        st.session_state.id_gsheet_reembolsos = '1dmcVUq7Bl_ipxPyxY8IcgxT7dCmTh_FLxYJqGigoSb0'
        
        st.session_state.lista_colunas_numero_df_reembolsos = ['Valor_Total']
        
        st.session_state.lista_colunas_data_df_reembolsos = ['Data_venc']
        
        st.session_state.meses_disponiveis = {'Janeiro': 1, 'Fevereiro': 2, 'Março': 3, 'Abril': 4, 'Maio': 5, 'Junho': 6, 'Julho': 7, 'Agosto': 8, 'Setembro': 9, 'Outubro': 10, 'Novembro': 11, 
                                              'Dezembro': 12}
        
        st.session_state.setores_desejados_gerencial = ['EVENTOS', 'GRUPOS', 'GUIA', 'HOTEL VENDAS', 'PDV', 'VENDAS ONLINE']
        
        st.session_state.setores_desejados_historico_por_vendedor = ['PDV', 'GUIA', 'HOTEL VENDAS', 'VENDAS ONLINE']
        
        st.session_state.combo_luck = ['CATAMARÃ DO FORRÓ', 'CITY TOUR', 'EMBARCAÇAO - CATAMARÃ DO FORRÓ ',  'EMBARCAÇÃO - ILHA DE AREIA VERMELHA', 'EMBARCAÇÃO - PASSEIO PELO RIO PARAÍBA', 
                                        'ILHA DE AREIA VERMELHA', 'EMBARCAÇÃO - PISCINAS DO EXTREMO ORIENTAL', 'ENTARDECER NA PRAIA DO JACARÉ ', 'LITORAL NORTE COM ENTARDECER NA PRAIA DO JACARÉ', 
                                        'PISCINAS DO EXTREMO ORIENTAL', 'PRAIAS DA COSTA DO CONDE']

st.title('Vendas Gerais por Setor')

st.divider()

if any(key not in st.session_state for key in ['df_reembolsos', 'df_metas_vendedor', 'df_metas', 'df_config', 'df_vendas_final', 'df_ranking', 'df_guias_in', 'df_paxs_in']):

    with st.spinner('Puxando reembolsos, metas de vendedores, metas de setores e configurações...'):

        gerar_df_reembolsos()

        gerar_df_metas_vendedor()

        gerar_df_metas()

        puxar_aba_simples(st.session_state.id_gsheet_metas_vendas, 'Configurações Vendas', 'df_config')

        st.session_state.passeios_incluidos = st.session_state.df_config[st.session_state.df_config['Configuração']=='Passeios Gráfico Pizza']['Parâmetro'].tolist()

    with st.spinner('Puxando vendas, ranking, guias IN e paxs IN do Phoenix...'):

        st.session_state.df_vendas_final = gerar_df_vendas_final()

        gerar_df_ranking()

        gerar_df_guias_in()

        gerar_df_paxs_in()

lista_setor = gerar_lista_setor()

col1, col2, col3 = st.columns([1.5, 3.0, 4.50])

with col1:

    with st.container():

        col1_1, col1_2 = st.columns(2)

        with col1_1:

            data_ini = st.date_input('Data Início', value=pd.to_datetime('2025-1-1'), format='DD/MM/YYYY', key='data_ini_on')

        with col1_2:
            
            data_fim = st.date_input('Data Fim', value=pd.to_datetime('2026-1-1'), format='DD/MM/YYYY', key='data_fim_on')  

    seleciona_setor = st.multiselect('Setor', sorted(lista_setor), default=None, key='seleciona_setor')

if len(seleciona_setor)>0:

    df_vendas, df_paxs_in, df_metas_vendedor, df_reembolsos, df_guias_in, df_metas_setor = filtrar_periodo_dfs()

    df_reembolsos = df_reembolsos.groupby('Vendedor', as_index=False)['Valor_Total'].sum()

    df_guias_in = df_guias_in.groupby('Guia', as_index=False)['Total_Paxs'].sum()

    if not '--- Todos ---' in seleciona_setor:

        df_vendas = df_vendas[df_vendas['Setor'].isin(seleciona_setor)]

    if len(df_vendas)>0:

        lista_vendedor, lista_canal, lista_hotel = criar_listas_vendedor_canal_hotel(df_vendas)

        if seleciona_setor[0]=='VENDAS ONLINE':

            seleciona_canal = col1.multiselect('Canal de Vendas', lista_canal, key='Can_on')

        else:

            seleciona_canal = []

        seleciona_vend = col1.multiselect('Vendedor', lista_vendedor, key='Ven_on')

        if seleciona_setor[0]=='HOTEL VENDAS':

            seleciona_hotel = col1.multiselect('Hotel', lista_hotel, key='Hot_on')

        else:

            seleciona_hotel = []

        df_vendas = filtrar_canal_vendedor_hotel_df_vendas(df_vendas, seleciona_canal, seleciona_vend, seleciona_hotel)

        df_hotel = gerar_df_hotel(df_vendas)

        df_vendas = ajustar_desconto_global(df_vendas)

        df_vendas_agrupado = gerar_df_vendas_agrupado(df_vendas, df_reembolsos, df_metas_vendedor, df_guias_in, df_paxs_in, df_metas_setor)

        df_cont_passeio = df_vendas.groupby(['Vendedor', 'Nome_Servico'], as_index=False)['Total Paxs'].sum()

        with col2:
                
            col2_1, col2_2 = st.columns([2,5])

            with col2_1:

                with st.container():

                    soma_vendas, tm_vendas, tm_setor_estip, total_desconto, paxs_recebidos, med_desconto = gerar_soma_vendas_tm_vendas_desconto_paxs_recebidos(df_vendas_agrupado, df_metas_setor)

                    meta_esperada_formatada, perc_alcancado = gerar_meta_esperada_perc_alcancado(soma_vendas, tm_setor_estip, paxs_recebidos)

                    plotar_quadrados_html('Valor Total Vendido', formatar_moeda(soma_vendas))

                    plotar_quadrados_html('Meta Estimada', meta_esperada_formatada)

                    plotar_quadrados_html('Meta de TM', formatar_moeda(tm_setor_estip))

                    plotar_quadrados_html('Total Descontos', formatar_moeda(total_desconto))

            with col2_2:
                    
                with st.container():

                    plotar_quadrados_html('% Alcancado', perc_alcancado)

                    plotar_quadrados_html('Paxs Recebidos', paxs_recebidos)

                    plotar_quadrados_html('Meta Atingida', formatar_moeda(tm_vendas))

                    plotar_quadrados_html('Media de Descontos', med_desconto)

        with col3:

            df_estilizado = gerar_df_estilizado(df_vendas_agrupado)

            st.dataframe(df_estilizado, hide_index=True, use_container_width=True)

            if seleciona_vend and seleciona_setor[0]=='HOTEL VENDAS' and len(seleciona_setor)==1:

                df_hotel[['Valor_Venda', 'Desconto Reserva x Serviços']] = df_hotel[['Valor_Venda', 'Desconto Reserva x Serviços']].applymap(formatar_moeda)

                st.dataframe(df_hotel[['Vendedor', 'Hotel', 'Valor_Venda']], hide_index=True, use_container_width=True)
            
            if len(seleciona_setor)==1 and seleciona_setor[0] == '--- Todos ---':

                df_setor_agrupado = df_vendas_agrupado[df_vendas_agrupado['Setor'].isin(st.session_state.setores_desejados_gerencial)].groupby('Setor', as_index=False)['Venda_Filtrada'].sum()

                if not df_setor_agrupado.empty:

                    fig = gerar_grafico_todos_setores(df_setor_agrupado)

            else:

                if len(seleciona_setor)==1 and seleciona_setor[0]=='GUIA':
                
                    df_vendas_grafico = df_vendas_agrupado[(pd.notna(df_vendas_agrupado['Paxs_IN'])) & (df_vendas_agrupado['Paxs_IN']>=20)]

                    fig = gerar_grafico_setor_especifico(df_vendas_grafico)

                else:

                    fig = gerar_grafico_setor_especifico(df_vendas_agrupado)

            st.plotly_chart(fig, key="key_1")

    else:

        st.error('O setor selecionado não possui venda nesse período')

row0 = st.columns(2)

passeios_incluidos = st.session_state.df_config[st.session_state.df_config['Configuração']=='Passeios Gráfico Pizza']['Parâmetro'].tolist()

if len(seleciona_setor)==1 and seleciona_setor[0] == '--- Todos ---':

    df_todos_vendedores_filtrado = gerar_df_todos_vendedores_filtrado(df_cont_passeio, passeios_incluidos)

    if not df_todos_vendedores_filtrado.empty:

        fig = gerar_grafico_pizza_todos_vendedores(df_todos_vendedores_filtrado, passeios_incluidos)

        st.plotly_chart(fig)

elif len(seleciona_setor)>0 and len(df_vendas)>0:

    coluna = 0

    for vendedor in df_cont_passeio['Vendedor'].unique():

        df_vendedor_filtrado = gerar_df_vendedor_filtrado(df_cont_passeio, passeios_incluidos, vendedor)
        
        if not df_vendedor_filtrado.empty:

            fig = gerar_grafico_pizza_vendedor(df_vendedor_filtrado, vendedor, passeios_incluidos)

            with row0[coluna%2]:
        
                st.plotly_chart(fig)

            coluna+=1
