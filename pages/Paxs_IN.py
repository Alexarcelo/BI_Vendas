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
from datetime import date
import sys
from pathlib import Path

def gerar_df_phoenix(base_luck, request_select):
    
    config = {
        'user': 'user_automation_jpa', 
        'password': 'luck_jpa_2024', 
        'host': 'comeia.cixat7j68g0n.us-east-1.rds.amazonaws.com', 
        'database': base_luck
        }

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

def tratar_df_paxs_in():

    df_paxs_in = st.session_state.df_paxs_in.copy()

    df_paxs_in = df_paxs_in[pd.notna(df_paxs_in['Data_Execucao'])]

    df_paxs_in['Ano'] = df_paxs_in['Ano'].astype(int)

    df_paxs_in['Mes_Nome'] = df_paxs_in['Mes_Ano'].dt.strftime('%B')

    df_paxs_in['Mes_Nome'] = df_paxs_in['Mes_Nome'].replace(st.session_state.meses_ingles_portugues)

    return df_paxs_in

def colher_ano_mes_selecao(df_paxs_in):

    lista_anos = df_paxs_in['Ano'].dropna().unique().tolist()

    lista_mes = [valor for valor in st.session_state.meses_disponiveis.keys() if valor in df_paxs_in['Mes_Nome'].unique()]

    ano_selecao = st.multiselect('Selecione o Ano:', options=lista_anos, default=[], key='paxs_real_001')

    mes_selecao = st.multiselect('Selecione o Mês', options=lista_mes, default=[], key='paxs_real_002')

    return ano_selecao, mes_selecao

def criar_df_paxs_in_filtrado_periodo(df):

    df_paxs_in = df.copy()

    if not 'Ano' in df_paxs_in.columns:

        df_paxs_in['Ano'] = pd.to_datetime(df_paxs_in['Data_Execucao']).dt.year

    if not 'Mes' in df_paxs_in.columns:
        
        df_paxs_in['Mes'] = pd.to_datetime(df_paxs_in['Data_Execucao']).dt.month

    if not 'Mes_Ano' in df_paxs_in.columns:
        
        df_paxs_in['Mes_Ano'] = pd.to_datetime(df_paxs_in['Data_Execucao']).dt.to_period('M')

    df_paxs_in = df_paxs_in[pd.notna(df_paxs_in['Data_Execucao'])]

    df_paxs_in['Ano'] = df_paxs_in['Ano'].astype(int)

    df_paxs_in['Mes_Nome'] = df_paxs_in['Mes_Ano'].dt.strftime('%B')

    df_paxs_in['Mes_Nome'] = df_paxs_in['Mes_Nome'].replace(st.session_state.meses_ingles_portugues)

    if len(mes_selecao)>0:

        df_paxs_in_filtrado_periodo = df_paxs_in[
            (df_paxs_in['Ano'].isin(ano_selecao)) &
            (df_paxs_in['Mes_Nome'].isin(mes_selecao))
        ]
    
    else:

        df_paxs_in_filtrado_periodo = df_paxs_in[
            df_paxs_in['Ano'].isin(ano_selecao)
        ]

    return df_paxs_in_filtrado_periodo

def plotar_grafico_paxs_in_por_hotel(df_paxs_in_filtrado_periodo):

    def gerar_graficos_hoteis(df_top15_hotel):

        df_top15_hotel['Total_Paxs'] = df_top15_hotel['Total_Paxs'].astype(int)

        fig_hotel = go.Figure()

        fig_hotel.add_trace(go.Bar(
            x=df_top15_hotel['Estabelecimento_Destino'],
            y=df_top15_hotel['Total_Paxs'],
            name='Maiores Hoteis - Por Fluxo',
            marker=dict(color='rgb(4,124,108)'),
            text=df_top15_hotel['Total_Paxs'],
            textposition='outside',
            textfont=dict(size=8, color='rgb(4,124,108)'),
            width=0.5,
        ))

        fig_hotel.update_layout(
            title='Maiores Hoteis - Por Fluxo',
            xaxis_title="Estabelecimento",
            yaxis_title="Total_Paxs",
            yaxis=dict(range=[0, df_top15_hotel['Total_Paxs'].max()*1.1]),
            template="plotly_white"
        )

        return fig_hotel

    df_paxs_in_por_hotel = df_paxs_in_filtrado_periodo.groupby(
        'Estabelecimento_Destino'
    ).agg(
        {
            'Total_Paxs': 'sum', 
            'Mes': 'first', 
            'Ano': 'first',
            'Mes_Nome': 'first',
            'Mes_Ano': 'first'
        }
    ).reset_index()

    df_top15_hotel = df_paxs_in_por_hotel.nlargest(15, 'Total_Paxs')

    fig_hotel = gerar_graficos_hoteis(df_top15_hotel)

    st.plotly_chart(fig_hotel, use_container_width=True)

def plotar_grafico_paxs_com_in_sem_in(mes_selecao, ano_selecao):

    def gerar_df_paxs_com_in():

        df_paxs_in_geral_filtrado_periodo = criar_df_paxs_in_filtrado_periodo(st.session_state.df_paxs_in_geral)

        df_paxs_in_geral_filtrado_periodo['Total_Paxs'] = df_paxs_in_geral_filtrado_periodo['Total_ADT'] + (df_paxs_in_geral_filtrado_periodo['Total_CHD']/2)

        df_paxs_in_mes = df_paxs_in_geral_filtrado_periodo.groupby(
            ['Mes_Ano','Ano', 'Mes'],
            as_index=False
        ).agg(
            {
                'Total_Paxs': 'sum'
            }
        )

        df_paxs_in_mes.rename(
            columns={'Total_Paxs': 'Paxs_Com_IN'}, 
            inplace=True
        )

        return df_paxs_in_mes

    def incluir_paxs_sem_in(mes_selecao, ano_selecao, df_paxs_in_mes):

        df_reservas_sem_in = st.session_state.df_servicos_por_reserva.copy()

        df_reservas_sem_in['Ano'] = pd.to_datetime(df_reservas_sem_in['Data_Execucao']).dt.year

        df_reservas_sem_in['Mes'] = pd.to_datetime(df_reservas_sem_in['Data_Execucao']).dt.month

        df_reservas_sem_in['Mes_Nome'] = pd.to_datetime(df_reservas_sem_in['Data_Execucao']).dt.strftime('%B')

        df_reservas_sem_in['Mes_Nome'] = df_reservas_sem_in['Mes_Nome'].replace(st.session_state.meses_ingles_portugues)

        if len(mes_selecao)>0:

            df_reservas_sem_in = df_reservas_sem_in[
                (df_reservas_sem_in['Ano'].isin(ano_selecao)) &
                (df_reservas_sem_in['Mes_Nome'].isin(mes_selecao))
            ]

        else:

            df_reservas_sem_in = df_reservas_sem_in[
                df_reservas_sem_in['Ano'].isin(ano_selecao)
            ]

        lista_reservas_sem_in = list(set(df_reservas_sem_in['Reserva_Mae'].unique()) - set(st.session_state.df_paxs_in['Reserva_Mae'].unique()))

        df_reservas_sem_in = df_reservas_sem_in[df_reservas_sem_in['Reserva_Mae'].isin(lista_reservas_sem_in)][
            [
                'Reserva_Mae',
                'Total_ADT',
                'Total_CHD',
                'Ano',
                'Mes'
            ]
        ].drop_duplicates()

        df_reservas_sem_in = df_reservas_sem_in.groupby(
            ['Ano', 'Mes'],
            as_index=False
        ).agg(
            {
                'Total_ADT': 'sum',
                'Total_CHD': 'sum'
            }
        )

        df_reservas_sem_in['Total_Paxs'] = df_reservas_sem_in['Total_ADT'] + (df_reservas_sem_in['Total_CHD']/2)

        df_reservas_sem_in.rename(
            columns={'Total_Paxs': 'Paxs_Sem_IN'},
            inplace=True
        )

        df_reservas_sem_in['Mes_Ano'] = pd.to_datetime(df_reservas_sem_in['Ano'].astype(int).astype(str) + '-' + df_reservas_sem_in['Mes'].astype(int).astype(str) + '-01').dt.to_period('M')

        df_paxs_in_mes = df_paxs_in_mes.merge(
            df_reservas_sem_in[['Mes_Ano', 'Paxs_Sem_IN']],
            on='Mes_Ano',
            how='left'
        )

        return df_paxs_in_mes

    def gerar_grafico_paxs_in(df_todos_in):

        fig_paxs_in = go.Figure()

        df_todos_in = df_todos_in[df_todos_in['Mes_Ano']<=date.today().strftime('%Y/%m')]

        df_todos_in['Mes_Ano'] = df_todos_in['Mes_Ano'].dt.strftime('%m/%y')

        df_todos_in['Paxs_Sem_IN'] = df_todos_in['Paxs_Sem_IN'].astype(int)

        df_todos_in['Paxs_Com_IN'] = df_todos_in['Paxs_Com_IN'].astype(int)

        fig_paxs_in.add_trace(go.Scatter(
            x=df_todos_in['Mes_Ano'], 
            y=df_todos_in['Paxs_Sem_IN'], 
            mode='lines+markers+text', 
            name='PAXS_SEM_IN',
            line=dict(width=1, shape='spline'),
            text=df_todos_in['Paxs_Sem_IN'], 
            textfont=dict(size=10),
            textposition='top center',
            ))
        fig_paxs_in.add_trace(go.Scatter(
            x=df_todos_in['Mes_Ano'], 
            y=df_todos_in['Paxs_Com_IN'], 
            mode='lines+markers+text', 
            name='PAXS_COM_IN',
            line=dict(width=1, shape='spline'),
            text=df_todos_in['Paxs_Com_IN'], 
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

    df_paxs_in_mes = gerar_df_paxs_com_in()

    df_paxs_in_mes = incluir_paxs_sem_in(mes_selecao, ano_selecao, df_paxs_in_mes)

    fig_paxs_in = gerar_grafico_paxs_in(df_paxs_in_mes)

    st.plotly_chart(fig_paxs_in, use_container_width=True)

def plotar_grafico_operadoras(df_paxs_in, ano_selecao):

    def gerar_df_top5_operadora(df_paxs_in):

        df_paxs_in['Total_Paxs'] = df_paxs_in['Total_Paxs'].astype(int)

        df_top5_operadora = df_paxs_in.groupby(['Parceiro']).agg({'Total_Paxs': 'sum',}).reset_index()

        if st.session_state.base_luck == 'test_phoenix_joao_pessoa':

            df_top5_operadora = df_top5_operadora.query("Parceiro != 'LUCK JOÃO PESSOA - PDV'").nlargest(5, 'Total_Paxs')

        elif st.session_state.base_luck == 'test_phoenix_natal':

            df_top5_operadora = df_top5_operadora.query("Parceiro != 'LUCK NATAL - PDV'").nlargest(5, 'Total_Paxs')

        elif st.session_state.base_luck == 'test_phoenix_salvador':

            df_top5_operadora = df_top5_operadora.query("Parceiro != 'LUCK SALVADOR - PDV'").nlargest(5, 'Total_Paxs')

        elif st.session_state.base_luck == 'test_phoenix_noronha': 

            df_top5_operadora = df_top5_operadora.loc[~df_top5_operadora['Parceiro'].str.contains('ATALAIA', case=False, na=False)].nlargest(5, 'Total_Paxs')

        elif st.session_state.base_luck == 'test_phoenix_recife':

            df_top5_operadora = df_top5_operadora.query("Parceiro != 'LUCK RECIFE - PDV'").nlargest(5, 'Total_Paxs')

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

    df_top5_operadora =  gerar_df_top5_operadora(df_paxs_in)

    df_filtrado_operadora = df_paxs_in.groupby(['Parceiro', 'Mes_Ano']).agg({'Total_Paxs': 'sum', 'Mes': 'first', 'Ano': 'first', 'Mes_Nome': 'first'}).reset_index()

    lista_operadora = df_top5_operadora['Parceiro'].unique()
    
    df_filtrado_operadora_ano = df_filtrado_operadora[(df_filtrado_operadora['Ano'].isin(ano_selecao)) & (df_filtrado_operadora['Mes_Ano']<=date.today().strftime('%Y/%m'))]

    fig_operadoras = grafico_linha(lista_operadora, df_filtrado_operadora_ano)

    st.plotly_chart(fig_operadoras, use_container_width=True)

    return df_filtrado_operadora, df_filtrado_operadora_ano

def plotar_tabelas_operadoras_mensais(mes_selecao, df_filtrado_operadora_ano, df_filtrado_operadora):

    if len(mes_selecao)==0:

        mes_selecao = df_filtrado_operadora_ano['Mes_Nome'].unique().tolist()

    for mes in mes_selecao:

        df_operadora = df_filtrado_operadora[df_filtrado_operadora['Mes_Nome'] == mes]

        df_operadora = df_operadora.groupby(['Parceiro', 'Ano'], as_index=False)['Total_Paxs'].sum()

        df_operadora_final = pd.DataFrame(data=df_operadora['Parceiro'].unique(), columns=['Parceiro'])

        for ano in sorted(df_operadora['Ano'].unique()):

            df_ano = df_operadora[df_operadora['Ano']==ano]

            df_ano = df_ano.rename(columns={'Total_Paxs': str(ano)})

            df_operadora_final = pd.merge(df_operadora_final, df_ano[['Parceiro', str(ano)]], on='Parceiro', how='left')

        if len(df_operadora_final.columns)==3:

            lista_anos = sorted(df_operadora['Ano'].unique())

            df_operadora_final['Diferença %'] = df_operadora_final[f'{lista_anos[1]}'] / df_operadora_final[f'{lista_anos[0]}'] - 1

            df_operadora_final['Diferença %'] = df_operadora_final['Diferença %'].apply(lambda x: f'{x:.2%}' if pd.notna(x) else '0%')

        for coluna in df_operadora_final.columns:

            if coluna!='Parceiro':

                df_operadora_final[coluna] = df_operadora_final[coluna].fillna(0)

        df_operadora_final = df_operadora_final.sort_values(by=str(df_operadora['Ano'].max()), ascending=False)

        st.header(mes)

        st.dataframe(df_operadora_final, hide_index=True, use_container_width=True)

sys.path.append(str(Path(__file__).resolve().parent))

from Vendas_Gerais import puxar_df_config, gerar_df_paxs_in

st.title('Paxs IN')

st.divider()

lista_keys_fora_do_session_state = [
    item for item in [
        'df_config',
        'df_paxs_in',
        'df_paxs_in_geral',
        'df_servicos_por_reserva'
    ] if item not in st.session_state
]

if len(lista_keys_fora_do_session_state)>0:

    with st.spinner('Puxando dados do Google Drive...'):

        if 'df_config' in lista_keys_fora_do_session_state:

            puxar_df_config()

    with st.spinner('Puxando dados do Phoenix...'):

        if 'df_paxs_in' in lista_keys_fora_do_session_state:

            gerar_df_paxs_in()

        if 'df_paxs_in_geral' in lista_keys_fora_do_session_state:

            st.session_state.df_paxs_in_geral = gerar_df_phoenix(
                st.session_state.base_luck, 
                '''SELECT * FROM vw_paxs_in_geral'''
            )

        if 'df_servicos_por_reserva' in lista_keys_fora_do_session_state:

            st.session_state.df_servicos_por_reserva = gerar_df_phoenix(
                st.session_state.base_luck, 
                '''SELECT * FROM vw_servicos_por_reserva'''
            )

df_paxs_in = tratar_df_paxs_in()

ano_selecao, mes_selecao = colher_ano_mes_selecao(df_paxs_in)

if len(ano_selecao)>0:

    df_paxs_in_filtrado_periodo = criar_df_paxs_in_filtrado_periodo(st.session_state.df_paxs_in)

    plotar_grafico_paxs_in_por_hotel(df_paxs_in_filtrado_periodo)

    plotar_grafico_paxs_com_in_sem_in(mes_selecao, ano_selecao)

    df_filtrado_operadora, df_filtrado_operadora_ano = plotar_grafico_operadoras(df_paxs_in, ano_selecao)

    plotar_tabelas_operadoras_mensais(mes_selecao, df_filtrado_operadora_ano, df_filtrado_operadora)
