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
import sys
from pathlib import Path

def criar_df_vendas_agrupado():

    df_vendas = st.session_state.df_vendas_final

    if st.session_state.base_luck == 'test_phoenix_joao_pessoa':

        df_vendas = df_vendas[(df_vendas['Setor'] != 'LOGISTICA') & (~df_vendas['Estabelecimento_Origem'].isin(['SEM HOTEL ', 'AEROPORTO JOÃO PESSOA'])) & (df_vendas['Vendedor'] != 'ComeiaLabs')]

    df_vendas['Mes_Nome'] = df_vendas['Mes_Ano'].dt.strftime('%B')

    df_vendas['Mes_Nome'] = df_vendas['Mes_Nome'].replace(st.session_state.meses_ingles_portugues)

    df_vendas = df_vendas.drop_duplicates(subset='Reserva', keep='first')

    df_vendas = df_vendas[['Data_Venda', 'Reserva', 'Estabelecimento_Origem', 'Total Paxs', 'Mes_Ano', 'Mes', 'Ano', 'Mes_Nome']]

    df_vendas_agrupado = df_vendas.groupby(['Reserva', 'Estabelecimento_Origem']).agg({'Total Paxs': 'max', 'Mes_Ano': 'first', 'Data_Venda': 'first', 'Mes': 'first', 'Ano': 'first', 
                                                                                       'Mes_Nome': 'first'}).reset_index()
    
    return df_vendas_agrupado

def gerar_df_filtrado(df_vendas_agrupado, df_paxs_in):

    df_filtrado = df_vendas_agrupado[~df_vendas_agrupado['Reserva'].isin(df_paxs_in['Reserva'])]

    df_filtrado['Reserva'] = df_filtrado['Reserva'].str.replace(r'\.\d+$', '', regex=True)

    df_filtrado = df_filtrado[~df_filtrado['Reserva'].isin(df_paxs_in['Reserva'])]

    return df_filtrado

def colher_ano_mes_selecao(df_paxs_in):

    lista_anos = df_paxs_in['Ano'].dropna().unique().tolist()

    lista_mes = [valor for valor in st.session_state.meses_disponiveis.keys() if valor in df_paxs_in['Mes_Nome'].unique()]

    ano_selecao = st.multiselect('Selecione o Ano:', options=lista_anos, default=[], key='paxs_real_001')

    mes_selecao = st.multiselect('Selecione o Mês', options=lista_mes, default=[], key='paxs_real_002')

    return ano_selecao, mes_selecao

def gerar_df_filtrado_hotel(df_filtrado):

    if len(mes_selecao)>0:

        df_filtrado_data = df_filtrado[(df_filtrado['Ano'].isin(ano_selecao)) & (df_filtrado['Mes_Nome'].isin(mes_selecao))]

    else:

        df_filtrado_data = df_filtrado[df_filtrado['Ano'].isin(ano_selecao)]

    df_filtrado_hotel = df_filtrado_data.groupby('Estabelecimento_Origem').agg({'Total Paxs': 'sum', 'Mes': 'first', 'Ano': 'first'}).reset_index()

    return df_filtrado_hotel

def gerar_df_top5_operadora(df_paxs_in):

    df_top5_operadora = df_paxs_in.groupby(['Parceiro']).agg({'Total_Paxs': 'sum',}).reset_index()

    if st.session_state.base_luck == 'test_phoenix_joao_pessoa':

        df_top5_operadora = df_top5_operadora.query("Parceiro != 'LUCK JOÃO PESSOA - PDV'").nlargest(5, 'Total_Paxs')

    elif st.session_state.base_luck == 'test_phoenix_natal':

        df_top5_operadora = df_top5_operadora.query("Parceiro != 'LUCK NATAL - PDV'").nlargest(5, 'Total_Paxs')

    return df_top5_operadora

def grafico_linha(lista_operadoras, df):
    
    if isinstance(lista_operadoras, str):
        lista_operadoras = [lista_operadoras]
    
    fig = go.Figure()
    
    df_filtrado = df[df['Parceiro'].isin(lista_operadoras)]

    df_filtrado['Mes_Ano'] = df_filtrado['Mes_Ano'].dt.strftime('%m/%y')
    
    operadoras_ordenadas = df_filtrado.groupby('Parceiro')['Total_Paxs'].sum().sort_values(ascending=False).index.tolist()
    
    for operadora in operadoras_ordenadas:

        df_operadora = df_filtrado[df_filtrado['Parceiro'] == operadora]
        
        fig.add_trace(go.Scatter(
            x=df_operadora['Mes_Ano'],
            y=df_operadora['Total_Paxs'],
            mode='lines+markers+text',
            line=dict(width=1, shape='spline'),
            name=operadora,
            text=df_operadora['Total_Paxs'],
            textfont=dict(size=10),
            textposition='top center',
        ))

    fig.update_layout(
        title=' TOP 5 Operadoras',
        xaxis_title='Mês/Ano',
        yaxis_title='Total de Paxs',
        legend_title='Operadoras',
        template='plotly_white'
    )
    
    return fig

def gerar_graficos_hoteis(df_top15_hotel):

    fig_hotel = go.Figure()

    fig_hotel.add_trace(go.Bar(
        x=df_top15_hotel['Estabelecimento_Origem'],
        y=df_top15_hotel['Total Paxs'],
        name='Maiores Hoteis - Por Fluxo',
        marker=dict(color='rgb(4,124,108)'),
        text=df_top15_hotel['Total Paxs'],
        textposition='outside',
        textfont=dict(size=8, color='rgb(4,124,108)'),
        width=0.5,
    ))

    fig_hotel.update_layout(
        title='Maiores Hoteis - Por Fluxo',
        xaxis_title="Estabelecimento",
        yaxis_title="Total Paxs",
        template="plotly_white"
    )

    return fig_hotel

def gerar_df_todos_in(df_filtrado_mes, df_paxs_in_mes):

    df_todos_in = pd.merge(df_filtrado_mes, df_paxs_in_mes, on=['Mes_Ano'], how='right')

    df_todos_in = df_todos_in.rename(columns={'Total Paxs': 'Paxs_Sem_IN', 'Total_Paxs': 'Paxs_Com_IN'})

    df_todos_in['Paxs_Sem_IN'] = df_todos_in['Paxs_Sem_IN'].fillna(0)

    df_todos_in['Mes_Ano'] = df_todos_in['Mes_Ano'].dt.strftime('%m/%y')

    return df_todos_in

def gerar_grafico_paxs_in(df_todos_in):

    fig_paxs_in = go.Figure()

    df_todos_in_ano = df_todos_in[df_todos_in['Ano_x'].isin(ano_selecao)]

    fig_paxs_in.add_trace(go.Scatter(
        x=df_todos_in_ano['Mes_Ano'], 
        y=df_todos_in_ano['Paxs_Sem_IN'], 
        mode='lines+markers+text', 
        name='PAXS_SEM_IN',
        line=dict(width=1, shape='spline'),
        text=df_todos_in_ano['Paxs_Sem_IN'], 
        textfont=dict(size=10),
        textposition='top center',
        ))
    fig_paxs_in.add_trace(go.Scatter(
        x=df_todos_in_ano['Mes_Ano'], 
        y=df_todos_in_ano['Paxs_Com_IN'], 
        mode='lines+markers+text', 
        name='PAXS_COM_IN',
        line=dict(width=1, shape='spline'),
        text=df_todos_in_ano['Paxs_Com_IN'], 
        textfont=dict(size=10),
        textposition='top center',
        ))

    fig_paxs_in.update_layout(
        title="Comparativo Passageiros - COM TRANSFER / SEM TRANSFER",
        xaxis_title="Mes_Ano",
        yaxis_title="PAXS Count",
        legend_title="Tipo",
        template="plotly_white"
    )

    return fig_paxs_in

sys.path.append(str(Path(__file__).resolve().parent))

from Vendas_Gerais import puxar_df_config, gerar_df_metas, gerar_df_vendas_final, gerar_df_paxs_in

st.title('Paxs IN')

st.divider()

lista_keys_fora_do_session_state = [item for item in ['df_config', 'df_metas', 'df_vendas_final', 'df_paxs_in'] if item not in st.session_state]

if len(lista_keys_fora_do_session_state)>0:

    with st.spinner('Puxando dados do Google Drive...'):

        if 'df_config' in lista_keys_fora_do_session_state:

            puxar_df_config()

        if 'df_metas' in lista_keys_fora_do_session_state:

            gerar_df_metas()

    with st.spinner('Puxando dados do Phoenix...'):

        if 'df_vendas_final' in lista_keys_fora_do_session_state:

            st.session_state.df_vendas_final = gerar_df_vendas_final()

        if 'df_paxs_in' in lista_keys_fora_do_session_state:

            gerar_df_paxs_in()

df_vendas_agrupado = criar_df_vendas_agrupado()

df_paxs_in = st.session_state.df_paxs_in.copy()

df_paxs_in = df_paxs_in[pd.notna(df_paxs_in['Data_Execucao'])]

df_paxs_in['Ano'] = df_paxs_in['Ano'].astype(int)

df_paxs_in['Mes_Nome'] = df_paxs_in['Mes_Ano'].dt.strftime('%B')

df_paxs_in['Mes_Nome'] = df_paxs_in['Mes_Nome'].replace(st.session_state.meses_ingles_portugues)

df_filtrado = gerar_df_filtrado(df_vendas_agrupado, df_paxs_in)

df_paxs_in_mes = df_paxs_in.groupby('Mes_Ano').agg({'Total_Paxs': 'sum','Mes': 'first','Ano': 'first'}).reset_index()

ano_selecao, mes_selecao = colher_ano_mes_selecao(df_paxs_in)

if len(ano_selecao)>0 or len(mes_selecao)>0:

    df_filtrado_hotel = gerar_df_filtrado_hotel(df_filtrado)

    df_filtrado_mes = df_filtrado.groupby('Mes_Ano').agg({'Total Paxs': 'sum', 'Mes': 'first', 'Ano': 'first'}).reset_index()

    df_top5_operadora =  gerar_df_top5_operadora(df_paxs_in)

    df_filtrado_operadora = df_paxs_in.groupby(['Parceiro', 'Mes_Ano']).agg({'Total_Paxs': 'sum', 'Mes': 'first', 'Ano': 'first', 'Mes_Nome': 'first'}).reset_index()

    df_top15_hotel = df_filtrado_hotel.nlargest(15, 'Total Paxs')

    lista_operadora = df_top5_operadora['Parceiro'].unique()
    
    df_filtrado_operadora_ano = df_filtrado_operadora[(df_filtrado_operadora['Ano'].isin(ano_selecao)) & (df_filtrado_operadora['Mes_Ano']<=date.today().strftime('%Y/%m'))]

    fig_operadoras = grafico_linha(lista_operadora, df_filtrado_operadora_ano)

    fig_hotel = gerar_graficos_hoteis(df_top15_hotel)

    df_todos_in = gerar_df_todos_in(df_filtrado_mes, df_paxs_in_mes)

    fig_paxs_in = gerar_grafico_paxs_in(df_todos_in)

    st.plotly_chart(fig_hotel, use_container_width=True)

    st.plotly_chart(fig_paxs_in, use_container_width=True)

    st.plotly_chart(fig_operadoras, use_container_width=True)

    col1, col2, col3 = st.columns([4, 4, 4])

    for mes in mes_selecao:

        df_operadora = df_filtrado_operadora[df_filtrado_operadora['Mes_Nome'] == mes]

        df_operadora = df_operadora.groupby(['Parceiro', 'Ano'], as_index=False)['Total_Paxs'].sum()

        df_operadora_final = pd.DataFrame(data=df_operadora['Parceiro'].unique(), columns=['Parceiro'])

        for ano in df_operadora['Ano'].unique():

            df_ano = df_operadora[df_operadora['Ano']==ano]

            df_ano = df_ano.rename(columns={'Total_Paxs': str(ano)})

            df_operadora_final = pd.merge(df_operadora_final, df_ano[['Parceiro', str(ano)]], on='Parceiro', how='left')

        for coluna in df_operadora_final.columns:

            if coluna!='Parceiro':

                df_operadora_final[coluna] = df_operadora_final[coluna].fillna(0)

        df_operadora_final = df_operadora_final.sort_values(by=str(df_operadora['Ano'].max()), ascending=False)

        st.header(mes)

        st.dataframe(df_operadora_final, hide_index=True, use_container_width=True)
