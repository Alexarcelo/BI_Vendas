import streamlit as st
import pandas as pd
import mysql.connector
import decimal
import gspread
from google.oauth2 import service_account
import numpy as np
from babel.numbers import format_currency
import plotly.express as px
import plotly.graph_objects as go
import locale
from datetime import date

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

def gerar_df_historico_vendedor():

    puxar_aba_simples(st.session_state.id_gsheet_metas_vendas, 'BD - Historico_Vendedor', 'df_historico_vendedor')

    tratar_colunas_numero_df(st.session_state.df_historico_vendedor, st.session_state.lista_colunas_numero_df_historico_vendedor)

    tratar_colunas_data_df(st.session_state.df_historico_vendedor, st.session_state.lista_colunas_data_df_historico_vendedor)

    st.session_state.df_historico_vendedor['Mes_Ano'] = pd.to_datetime(st.session_state.df_historico_vendedor['Data']).dt.to_period('M')

    st.session_state.df_historico_vendedor['Setor'] = st.session_state.df_historico_vendedor['Vendedor'].str.split(' - ').str[1]

    st.session_state.df_historico_vendedor['Ticket_Medio'] = st.session_state.df_historico_vendedor['Valor'] / st.session_state.df_historico_vendedor['Paxs_Total']

    st.session_state.df_historico_vendedor['Venda_Esperada'] = st.session_state.df_historico_vendedor['Paxs_Total'] * st.session_state.df_historico_vendedor['Meta']

def gerar_df_metas():

    puxar_aba_simples(st.session_state.id_gsheet_metas_vendas, 'BD - Metas', 'df_metas')

    tratar_colunas_numero_df(st.session_state.df_metas, st.session_state.lista_colunas_numero_df_metas)

    tratar_colunas_data_df(st.session_state.df_metas, st.session_state.lista_colunas_data_df_metas)

    st.session_state.df_metas['Mes_Ano'] = pd.to_datetime(st.session_state.df_metas['Data']).dt.to_period('M')

    st.session_state.df_metas['Meta_Total'] = st.session_state.df_metas['Meta_Guia'] + st.session_state.df_metas['Meta_PDV'] + st.session_state.df_metas['Meta_HV'] + \
        st.session_state.df_metas['Meta_Grupos'] + st.session_state.df_metas['Meta_VendasOnline']

def gerar_df_metas_vendedor():

    puxar_aba_simples(st.session_state.id_gsheet_metas_vendas, 'BD - Metas_Vendedor', 'df_metas_vendedor')

    tratar_colunas_numero_df(st.session_state.df_metas_vendedor, st.session_state.lista_colunas_numero_df_metas_vendedor)

    tratar_colunas_data_df(st.session_state.df_metas_vendedor, st.session_state.lista_colunas_data_df_metas_vendedor)

    st.session_state.df_metas_vendedor['Mes_Ano'] = pd.to_datetime(st.session_state.df_metas_vendedor['Data']).dt.to_period('M')

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

    request_select = '''SELECT `Tipo de Servico`, `Data Execucao`, `Servico`, `Status do Servico`, `Total ADT`, `Total CHD` 
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

def gerar_df_paxs_mes():

    df_paxs_in = st.session_state.df_paxs_in.groupby(['Mes_Ano'], as_index=False)['Total_Paxs'].sum()

    df_paxs_mes = pd.merge(df_paxs_in, st.session_state.df_metas, on=['Mes_Ano'], how='left')

    df_paxs_mes['Paxs_Real'] = df_paxs_mes['Total_Paxs'].fillna(0) + df_paxs_mes['Paxs_Desc'].fillna(0)

    return df_paxs_mes

def gerar_df_vendas(df_paxs_mes, df_guias_in):

    def gerar_df_vendas_agrupado():

        df_vendas = st.session_state.df_vendas_final.copy()

        df_vendas = df_vendas[df_vendas['Setor'].isin(st.session_state.setores_desejados_historico_por_vendedor)]

        df_vendas['Desconto_Global_Ajustado'] = df_vendas.apply(lambda row: row['Desconto_Global_Por_Servico'] if pd.notna(row['Desconto_Global_Por_Servico']) and 
                                                                row['Desconto_Global_Por_Servico'] < 1000 and row['Nome_Servico'] != 'EXTRA' else 0, axis=1)

        df_vendas = df_vendas.groupby(['Vendedor', 'Mes_Ano'], dropna=False, as_index=False).agg({'Valor_Venda': 'sum', 'Desconto_Global_Por_Servico': 'sum', 'Meta': 'first', 'Ano': 'mean', 
                                                                                                'Mes': 'mean', 'Setor': 'first'})
        
        return df_vendas
    
    def adicionar_valor_reembolsos_paxs_real_paxs_in_meta_vendedor(df_paxs_mes, df_guias_in, df_vendas):
    
        df_reembolso = st.session_state.df_reembolsos.groupby(['Vendedor', 'Mes_Ano'], as_index=False)['Valor_Total'].sum()

        df_vendas = pd.merge(df_vendas, df_reembolso, on=['Vendedor', 'Mes_Ano'], how='left')

        df_vendas = pd.merge(df_vendas, df_paxs_mes[['Paxs_Real', 'Mes_Ano']], on='Mes_Ano', how='left')

        df_guias_in = df_guias_in.rename(columns={'Guia': 'Vendedor'})

        df_vendas = pd.merge(df_vendas, df_guias_in[['Vendedor', 'Mes_Ano','Total_Paxs']], on=['Vendedor', 'Mes_Ano'], how='left')

        df_vendas = pd.merge(df_vendas, st.session_state.df_metas_vendedor[['Vendedor', 'Setor', 'Mes_Ano', 'Meta_Mes']], on=['Vendedor', 'Setor', 'Mes_Ano'], how='left')

        return df_vendas
    
    def ajustar_colunas_meta_mes_total_paxs_venda_filtrada_ticket_medio_venda_esperada(df_vendas):

        df_vendas['Meta_Mes'] = df_vendas['Meta_Mes'].fillna(df_vendas['Meta'])

        df_vendas['Total_Paxs'] = df_vendas['Total_Paxs'].fillna(0)

        df_vendas.loc[df_vendas['Setor'] != 'GUIA', 'Total_Paxs'] = df_vendas['Paxs_Real']

        df_vendas['Venda_Filtrada'] = df_vendas['Valor_Venda'].fillna(0) - df_vendas['Valor_Total'].fillna(0)

        df_vendas['Ticket_Medio'] = df_vendas['Venda_Filtrada'] / df_vendas['Total_Paxs']

        df_vendas['Venda_Esperada'] = df_vendas['Total_Paxs'] * df_vendas['Meta_Mes']

        return df_vendas
    
    df_vendas = gerar_df_vendas_agrupado()

    df_vendas = adicionar_valor_reembolsos_paxs_real_paxs_in_meta_vendedor(df_paxs_mes, df_guias_in, df_vendas)

    df_vendas = ajustar_colunas_meta_mes_total_paxs_venda_filtrada_ticket_medio_venda_esperada(df_vendas)

    return df_vendas

def concatenar_vendas_com_historico_vendedor(df_vendas):

    df_phoenix_vendedor = df_vendas[['Vendedor', 'Setor', 'Venda_Filtrada', 'Meta_Mes', 'Total_Paxs', 'Mes_Ano', 'Ticket_Medio', 'Venda_Esperada']]

    df_historico_vendedor = st.session_state.df_historico_vendedor[(st.session_state.df_historico_vendedor['Mes_Ano'] != '2024-04')]\
        [['Vendedor', 'Setor', 'Valor', 'Meta', 'Paxs_Total', 'Mes_Ano', 'Ticket_Medio', 'Venda_Esperada']]

    df_historico_vendedor = df_historico_vendedor.rename(columns={'Valor': 'Venda_Filtrada', 'Meta': 'Meta_Mes', 'Paxs_Total': 'Total_Paxs'})

    df_geral_vendedor_1 = pd.concat([df_historico_vendedor, df_phoenix_vendedor], ignore_index=True)

    return df_geral_vendedor_1

def agrupar_ajustar_colunas_df_geral_vendedor(df_geral_vendedor_1):

    df_geral_vendedor = df_geral_vendedor_1.groupby(['Vendedor', 'Mes_Ano'], as_index=False).agg({'Setor': 'first', 'Venda_Filtrada': 'sum', 'Meta_Mes': 'min', 'Total_Paxs': 'min', 
                                                                                                  'Ticket_Medio': 'sum', 'Venda_Esperada': 'min'})

    df_geral_vendedor['Ticket_Medio'] = np.where(df_geral_vendedor['Total_Paxs']!=0, df_geral_vendedor['Venda_Filtrada'] / df_geral_vendedor['Total_Paxs'], 0)

    df_geral_vendedor['Performance_Mes'] = df_geral_vendedor['Venda_Filtrada'] / df_geral_vendedor['Venda_Esperada']

    df_geral_vendedor['Ano'] = df_geral_vendedor['Mes_Ano'].dt.year

    return df_geral_vendedor

def adicionar_performance_anual_acumulado_anual_meta_anual(df_geral_vendedor):

    performance_anual = df_geral_vendedor.groupby(['Vendedor', 'Ano']).apply(lambda x: x['Venda_Filtrada'].sum() / x['Venda_Esperada'].sum() if x['Venda_Esperada'].sum() != 0 else 0)\
        .reset_index(name='Performance_Anual')

    acumulado_anual = df_geral_vendedor.groupby(['Vendedor', 'Ano']).apply(lambda x: x['Venda_Filtrada'].sum()).reset_index(name='Acumulado_Anual')

    meta_anual = df_geral_vendedor.groupby(['Vendedor', 'Ano']).apply(lambda x: x['Venda_Esperada'].sum()).reset_index(name='Meta_Anual')

    df_geral_vendedor = pd.merge(df_geral_vendedor, performance_anual, on=['Vendedor', 'Ano'], how='left')

    df_geral_vendedor = pd.merge(df_geral_vendedor, acumulado_anual, on=['Vendedor', 'Ano'], how='left')

    df_geral_vendedor = pd.merge(df_geral_vendedor, meta_anual, on=['Vendedor', 'Ano'], how='left')

    return df_geral_vendedor

def formatar_moeda(valor):

    return format_currency(valor, 'BRL', locale='pt_BR')

def gerar_grafico_vendedor(vendedor, df):
    
    df_vendedor = df[df['Vendedor'] == vendedor]
    
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df_vendedor['Mes_Ano'],
        y=df_vendedor['Venda_Filtrada'],
        name='Venda Filtrada',
        marker=dict(color='rgb(4,124,108)'),
        text=df_vendedor['Venda_Filtrada'].apply(formatar_moeda),
        textposition='outside',
        textfont=dict(size=10)
    ))
    
    fig.add_trace(go.Scatter(
        x=df_vendedor['Mes_Ano'],
        y=df_vendedor['Ticket_Medio'],
        mode='lines+markers+text',
        line=dict(color='orange', width=1, shape='spline'),
        name='Ticket Médio',
        text=df_vendedor['Ticket_Medio'].apply(formatar_moeda),
        textfont=dict(size=10, color='orange'),
        textposition='top center',
        yaxis='y2'
    ))
    
    fig.add_trace(go.Scatter(
        x=df_vendedor['Mes_Ano'],
        y=df_vendedor['Meta_Mes'],
        mode='lines+markers+text',
        line=dict(color='blue', width=1, shape='spline'),
        name='Meta',
        text=df_vendedor['Meta_Mes'].apply(formatar_moeda),
        textfont=dict(size=10, color='blue'),
        textposition='top center',
        yaxis='y2'
    ))
    
    fig.update_layout(
        title=f"Anual - {vendedor}",
        xaxis_title="Mês/Ano",
        yaxis_title="Valores",
        yaxis=dict(showgrid=False, range=[0, df_vendedor['Venda_Filtrada'].max() * 2]),
        yaxis2=dict(
            title='Valores (TM e META)',
            overlaying='y',
            side='right',
            showgrid=False,
            range=[0, df_vendedor['Ticket_Medio'].max()*1.05]
        ), 
        barmode='group',
        legend=dict(
            font=dict(size=8),
            orientation="h",
            yanchor="bottom",
            y=1.1,
            xanchor="left",
            x=0.6      
        )
    )
    
    return fig

def gerar_grafico_acumulado_meta(vendedor, df):
    df_vendedor = df[df['Vendedor'] == vendedor]
    df_anual = df_vendedor.groupby('Ano').agg({
        'Acumulado_Anual': 'mean',
        'Meta_Anual': 'mean'
    }).reset_index()
    df_anual['Performance'] = df_anual['Acumulado_Anual'] / df_anual['Meta_Anual']
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df_anual['Ano'],
        y=df_anual['Acumulado_Anual'],
        name='Acumulado Anual',
        marker=dict(color='rgb(4,124,108)'),
        text=df_anual['Acumulado_Anual'].apply(formatar_moeda),
        textposition='outside',
        textfont=dict(size=10),
        width=0.3,
    ))
    fig.add_trace(go.Bar(
        x=df_anual['Ano'],
        y=df_anual['Meta_Anual'],
        name='Meta Anual',
        marker=dict(color='steelblue'),
        text=df_anual['Meta_Anual'].apply(formatar_moeda),
        textposition='outside',
        textfont=dict(size=10),
        width=0.3,             # Define a largura das barras
    ))
    fig.update_layout(
        title=f"Acumulado - {vendedor} | Performance - {df_anual['Performance'].loc[0] * 100:.2f}%",
        xaxis_title="Ano",
        yaxis_title="Valores",
        barmode='group',
        legend=dict(
            font=dict(size=8),
            orientation="h",             # Legenda em orientação horizontal
            yanchor="top",            # Âncora na parte inferior da legenda
            y=1.15,                       # Posição vertical acima da área de plotagem
            xanchor="left",            # Âncora no centro horizontal da legenda
            x=-0.2      
        )
    )
    return fig

st.set_page_config(layout='wide')

row_titulo = st.columns(1)

tipo_analise = st.radio('Análise', ['Acompanhamento Anual - Vendedores', 'Historico por Vendedor'], index=None)

if tipo_analise:

    with row_titulo[0]:

        st.title(tipo_analise)

        st.divider()

if any(key not in st.session_state for key in ['df_reembolsos', 'df_config', 'df_historico_vendedor', 'df_metas', 'df_metas_vendedor', 'df_vendas_final', 'df_guias_in', 'df_paxs_in']):

    with st.spinner('Puxando reembolsos, configurações, histórico...'):

        gerar_df_reembolsos()

        puxar_aba_simples(st.session_state.id_gsheet_metas_vendas, 'Configurações Vendas', 'df_config')

        gerar_df_historico_vendedor()

        gerar_df_metas()

        gerar_df_metas_vendedor()

    with st.spinner('Puxando vendas, ranking, guias IN e paxs IN do Phoenix...'):

        st.session_state.df_vendas_final = gerar_df_vendas_final()

        gerar_df_guias_in()

        gerar_df_paxs_in()

locale.setlocale(locale.LC_TIME, 'pt_BR.UTF-8')

if not 'df_geral_vendedor' in st.session_state:

    df_paxs_mes = gerar_df_paxs_mes()

    df_guias_in = st.session_state.df_guias_in.groupby(['Guia', 'Mes_Ano'], as_index=False)['Total_Paxs'].sum()

    df_vendas = gerar_df_vendas(df_paxs_mes, df_guias_in)

    df_geral_vendedor_1 = concatenar_vendas_com_historico_vendedor(df_vendas)

    df_geral_vendedor = agrupar_ajustar_colunas_df_geral_vendedor(df_geral_vendedor_1)

    st.session_state.df_geral_vendedor = adicionar_performance_anual_acumulado_anual_meta_anual(df_geral_vendedor)

if tipo_analise=='Historico por Vendedor':

    lista_anos = st.session_state.df_geral_vendedor['Ano'].unique().tolist()

    col1, col2 = st.columns([4, 8])

    with col1:

        ano_selecao = st.multiselect('Selecione o Ano:', options=lista_anos, default=[], key='vend_0001')

    with col2:

        setor_selecao = st.multiselect('Selecione o Setor:', options=st.session_state.setores_desejados_historico_por_vendedor, default=[], key='vend_0002')

    if len(ano_selecao)>0 and len(setor_selecao)>0:

        df_filtrado = st.session_state.df_geral_vendedor[(st.session_state.df_geral_vendedor['Ano'].isin(ano_selecao)) & (st.session_state.df_geral_vendedor['Setor'].isin(setor_selecao))]

        df_filtrado['Mes_Ano'] = df_filtrado['Mes_Ano'].dt.strftime('%m/%y')

        vendedores = df_filtrado['Vendedor'].unique()

        for vendedor in vendedores:

            with col1:

                fig_anual = gerar_grafico_acumulado_meta(vendedor, df_filtrado)

                st.plotly_chart(fig_anual)

            with col2:

                fig_mensal = gerar_grafico_vendedor(vendedor, df_filtrado)

                st.plotly_chart(fig_mensal)

elif tipo_analise=='Acompanhamento Anual - Vendedores':

    lista_anos = st.session_state.df_geral_vendedor['Ano'].unique().tolist()

    ano_atual = date.today().year

    df_filtro_lista = st.session_state.df_geral_vendedor.groupby(['Vendedor'], as_index=False)['Venda_Filtrada'].sum()

    lista_vendedor = df_filtro_lista['Vendedor'].unique().tolist()

    top_vendedores = df_filtro_lista.nlargest(5, 'Venda_Filtrada')['Vendedor'].tolist()

    col1, col2 = st.columns([4, 8])

    with col1:

        ano_selecao = st.multiselect('Selecione o Ano:', options=lista_anos, default=ano_atual, key='perf_0001')

    with col2:

        vendedor_selecao = st.multiselect('Selecione o Vendedor:', options=lista_vendedor, default=top_vendedores, key='perf_0002')

    if len(ano_selecao)>0 and len(vendedor_selecao)>0:

        df_filtrado = st.session_state.df_geral_vendedor[(st.session_state.df_geral_vendedor['Ano'].isin(ano_selecao)) & (st.session_state.df_geral_vendedor['Vendedor'].isin(vendedor_selecao))]

        df_filtrado['Mes_Ano'] = df_filtrado['Mes_Ano'].dt.strftime('%m/%y')

        vendedores = df_filtrado['Vendedor'].unique()

        for vendedor in vendedores:

            fig_mensal = gerar_grafico_vendedor(vendedor, df_filtrado)

            st.plotly_chart(fig_mensal)
