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
import sys
from pathlib import Path

def gerar_df_historico():

    puxar_aba_simples(st.session_state.id_gsheet_metas_vendas, 'BD - Historico', 'df_historico')

    tratar_colunas_numero_df(st.session_state.df_historico, st.session_state.lista_colunas_numero_df_historico)
    
    st.session_state.df_historico['Mes_Ano'] = pd.to_datetime(st.session_state.df_historico['Ano'].astype(str) + '-' + st.session_state.df_historico['Mes'].astype(str) + '-01').dt.to_period('M')
   
def gerar_df_agrupado():

    df_vendas = st.session_state.df_vendas_final.copy()

    df_vendas['Valor_Real'] = df_vendas['Valor_Venda'].fillna(0) - df_vendas['Valor_Reembolso'].fillna(0)

    df_vendas_setor = df_vendas.groupby(['Setor', 'Mes_Ano'])['Valor_Real'].sum().reset_index()

    df_paxs_in = st.session_state.df_paxs_in.groupby(['Mes_Ano'])['Total_Paxs'].sum().reset_index()

    df_agrupado = pd.merge(df_vendas_setor, df_paxs_in[['Mes_Ano', 'Total_Paxs']], on='Mes_Ano', how='left')

    if st.session_state.base_luck == 'test_phoenix_joao_pessoa':

        df_historico_setor = st.session_state.df_historico.groupby(['Setor', 'Mes_Ano']).agg({'Valor_Venda': 'sum', 'Paxs ADT': 'mean'}).reset_index()

        df_agrupado = pd.merge(df_historico_setor, df_agrupado, on=['Setor', 'Mes_Ano'], how='outer')

    df_agrupado['Ano'] = df_agrupado['Mes_Ano'].dt.year

    df_agrupado['Mes'] = df_agrupado['Mes_Ano'].dt.month

    if st.session_state.base_luck == 'test_phoenix_joao_pessoa':

        df_agrupado['Paxs'] = df_agrupado[['Paxs ADT', 'Total_Paxs']].max(axis=1)

        df_agrupado = df_agrupado.drop(columns=['Paxs ADT', 'Total_Paxs'])

        df_agrupado['Valor_Total'] = df_agrupado['Valor_Venda'].fillna(0) + df_agrupado['Valor_Real'].fillna(0)

    else:

        df_agrupado['Paxs'] = df_agrupado['Total_Paxs']

        df_agrupado = df_agrupado.drop(columns=['Total_Paxs'])

        df_agrupado['Valor_Total'] = df_agrupado['Valor_Real'].fillna(0)

        if st.session_state.base_luck == 'test_phoenix_natal':

            df_agrupado = df_agrupado[pd.notna(df_agrupado['Paxs'])]

    df_agrupado['Ticket_Medio'] = df_agrupado['Valor_Total'] / df_agrupado['Paxs']

    df_agrupado['Mes_Ano'] = df_agrupado['Mes_Ano'].astype(str)

    return df_agrupado

def formatar_moeda(valor):

    return format_currency(valor, 'BRL', locale='pt_BR')

def plotar_graficos_linha_por_setor(setores, df_agrupado):

    colunas = st.columns(2)

    i = 0

    for setor in setores:

        df_setor = df_agrupado[df_agrupado['Setor'] == setor]

        fig = go.Figure()

        fig.add_trace(go.Bar(
            x=df_setor['Mes_Ano'],
            y=df_setor['Valor_Total'],
            name='Valor Total',
            text=df_setor['Valor_Total'].apply(formatar_moeda),
            textposition='outside',
            textfont=dict(size=10),
            marker=dict(color='#047c6c')
        ))

        fig.add_trace(go.Scatter(
            x=df_setor['Mes_Ano'],
            y=df_setor['Ticket_Medio'],
            mode='lines+markers+text',
            name='Ticket Médio',
            text=df_setor['Ticket_Medio'].apply(formatar_moeda),
            textposition='top center',
            textfont=dict(size=10, color='orange'),
            line=dict(color='orange', width=2, shape='spline'),
            yaxis='y2'
        ))

        fig.update_layout(
            title=f'Valor Total e TM para o Setor: {setor}',
            yaxis_title='Valor Total',
            xaxis_title='Mês/Ano',
            yaxis=dict(
                range=[00, 1500000],
                showgrid=False,
            ),
            yaxis2=dict(
                title='Ticket Médio',
                overlaying='y',
                side='right',
                showgrid=False,
            ),
            xaxis=dict(
                showgrid=False
            )
        )

        colunas[i%2].plotly_chart(fig)

        i+=1

def plotar_graficos_barra_valor_total_por_setor(df_vendas_mensal):

    meses = df_vendas_mensal['Mes'].unique()

    colunas = st.columns(2)

    i=0

    for mes in meses:

        df_mes = df_vendas_mensal[df_vendas_mensal['Mes'] == mes]

        mes_nome = next((chave for chave, valor in st.session_state.meses_disponiveis.items() if valor == mes), None)

        df_mes['Ano'] = df_mes['Ano'].astype(str)

        df_mes['Mes'] = df_mes['Mes'].astype(str)
        
        fig = px.bar(
            df_mes, 
            x='Setor', 
            y='Valor_Total', 
            color='Ano',
            title=f'Valor Total por Setor Ano a Ano - Ref. {mes_nome}',
            labels={'Valor_Total': 'Valor Total', 'Setor': 'Setor'},
            text=df_mes['Valor_Total'].apply(formatar_moeda),
            color_discrete_sequence=['#047c6c', '#3CB371', '#90EE90'],
            barmode='group'
            )
        fig.update_traces(
            textposition='outside',
            textfont=dict(size=10)
        )
        fig.update_layout(
            bargap=0.2,
            bargroupgap=0.1,
            xaxis=dict(showgrid=False),
            yaxis=dict(showgrid=False)
        )
        colunas[i%2].plotly_chart(fig)

        i+=1

def plotar_grafico_barra_valor_total_por_setor_ano_resumo(df_vendas_anual):

    df_vendas_anual['nomes_barras'] = df_vendas_anual.apply(lambda row: f"{formatar_moeda(row['Valor_Total'])}<br>Variação: {round(row['Variacao_Anual'], 2)}%" 
                                   if pd.notna(row['Variacao_Anual'])
                                   else f"{formatar_moeda(row['Valor_Total'])}", axis=1)

    df_vendas_anual['Ano'] = df_vendas_anual['Ano'].astype(str)

    df_vendas_anual = df_vendas_anual.reset_index(drop=True)

    fig_ano = px.bar(df_vendas_anual, x='Setor', y='Valor_Total', color='Ano',
                title=f'Valor Total por Setor Ano a Ano',
                labels={'Valor_Total': 'Valor Total', 'Setor': 'Setor'},
                text=df_vendas_anual['nomes_barras'],
                color_discrete_sequence=['#047c6c', '#3CB371', '#90EE90'],
                barmode='group',
                category_orders={"Ano": sorted(df_vendas_anual['Ano'].unique())}
                )
    fig_ano.update_traces(
        textposition='outside',
        textfont=dict(size=10)
    )
    fig_ano.update_layout(
        bargap=0.2,
        bargroupgap=0.1,
        xaxis=dict(showgrid=False),
        yaxis=dict(title='Valor Total', showgrid=False, range=[0, df_vendas_anual['Valor_Total'].max() * 1.5]),
        yaxis2=dict(
            title='Variação (%)',
            overlaying='y',
            side='right',
            showgrid=False,
            range=[-100, 100]
        )
    )

    st.plotly_chart(fig_ano)

def plotar_grafico_fluxo_paxs(df_filtro_paxs):

    total_paxs = df_filtro_paxs.groupby('Mes_Ano')['Paxs'].sum().reset_index()

    total_paxs['Variacao_Percentual'] = total_paxs['Paxs'].pct_change() * 100

    fig_fluxo = px.bar(total_paxs,
                x='Mes_Ano', 
                y='Paxs', 
                title='Total de Paxs por Mes_Ano',
                labels={'Paxs': 'Total de Paxs'},
                color_discrete_sequence=['#047c6c', '#3CB371', '#90EE90'],
                text='Paxs')
    fig_fluxo.update_traces(
                textposition='outside',
                textfont=dict(size=10)
                )
    fig_fluxo.update_yaxes(range=[-500, 10000])

    fig_fluxo.add_scatter(x=total_paxs['Mes_Ano'], 
                    y=total_paxs['Variacao_Percentual'], 
                    mode='lines+markers+text', 
                    line=dict(color='green', width=1),
                    name='Variação %', 
                    line_shape='spline',  # Suaviza a linha
                    text=[f"{round(val, 2)}%" for val in total_paxs['Variacao_Percentual']],
                    yaxis='y2',
                    textposition='top center')

    fig_fluxo.update_layout(yaxis2=dict(overlaying='y', 
                    side='right', 
                    title='Variação %',
                    range=[-500, 100],
                    showgrid=False
                    ),
                    xaxis=dict(showgrid=False),
                    yaxis=dict(showgrid=False)
        )

    st.plotly_chart(fig_fluxo)

def plotar_grafico_ticket_medio(df_filtro_receita):

    fig_tm = go.Figure()

    fig_tm.add_trace(go.Bar(
        x=df_filtro_receita['Mes_Ano'],
        y=df_filtro_receita['Valor_Total'],
        name='Valor Total',
        text=df_filtro_receita['Valor_Total'].apply(formatar_moeda),
        marker=dict(color='rgb(4,124,108)'),
        textposition='outside',
        textfont=dict(size=12),
    ))

    fig_tm.add_trace(go.Scatter(
        x=df_filtro_receita['Mes_Ano'],
        y=df_filtro_receita['Ticket_Medio'] * 10000,  # Ajuste de escala para visualização
        name='Ticket Médio',
        mode='lines+markers+text',
        line=dict(color='rgb(4,124,108)', width=1, shape='spline'),
        textposition='top center',
        text=df_filtro_receita['Ticket_Medio'].apply(formatar_moeda),
        textfont=dict(size=10)
    ))

    fig_tm.add_trace(go.Scatter(
        x=df_filtro_receita['Mes_Ano'],
        y=df_filtro_receita['Variacao_Percentual'] * 20000,  # Ajuste de escala para visualização
        name='Variação Percentual',
        mode='lines+markers+text',
        line=dict(color='orange', width=1, shape='spline'),
        text=[f"{round(val, 2)}%" for val in df_filtro_receita['Variacao_Percentual']],
        textfont=dict(size=8, color='orange'),
        textposition='top center',
    ))

    fig_tm.update_layout(
        title='Valor Total, Ticket Médio e Variação Percentual',
        yaxis_title='Valor',
        xaxis=dict(title='Mês/Ano', showgrid=False),
        yaxis=dict(showgrid=False),
        yaxis2=dict(
            title='Ticket Médio e Variação Percentual',
            overlaying='y',
            side='right',
            showgrid=False
        ),
        barmode='group'
    )

    st.plotly_chart(fig_tm)

st.set_page_config(layout='wide')

sys.path.append(str(Path(__file__).resolve().parent.parent))

from Vendas_Gerais import puxar_aba_simples, tratar_colunas_numero_df, puxar_df_config, gerar_df_metas, gerar_df_vendas_final, gerar_df_paxs_in

st.title('Gerencial - Ano a Ano')

st.divider()

if st.session_state.base_luck == 'test_phoenix_joao_pessoa':

    lista_keys_fora_do_session_state = [item for item in ['df_config', 'df_historico', 'df_metas', 'df_vendas_final', 'df_paxs_in'] if item not in st.session_state]
    
    if len(lista_keys_fora_do_session_state)>0:

        with st.spinner('Puxando configurações, histórico e metas de setor...'):

            if 'df_config' in lista_keys_fora_do_session_state:

                puxar_df_config()

            if 'df_historico' in lista_keys_fora_do_session_state:

                gerar_df_historico()

            if 'df_metas' in lista_keys_fora_do_session_state:

                gerar_df_metas()

        with st.spinner('Puxando vendas e paxs IN do Phoenix...'):

            if 'df_vendas_final' in lista_keys_fora_do_session_state:

                st.session_state.df_vendas_final = gerar_df_vendas_final()

            if 'df_paxs_in' in lista_keys_fora_do_session_state:

                gerar_df_paxs_in()

elif st.session_state.base_luck == 'test_phoenix_natal':

    lista_keys_fora_do_session_state = [item for item in ['df_config', 'df_metas', 'df_vendas_final', 'df_paxs_in'] if item not in st.session_state]
    
    if len(lista_keys_fora_do_session_state)>0:

        with st.spinner('Puxando configurações e metas de setor...'):

            if 'df_config' in lista_keys_fora_do_session_state:

                puxar_df_config()

            if 'df_metas' in lista_keys_fora_do_session_state:

                gerar_df_metas()

        with st.spinner('Puxando vendas e paxs IN do Phoenix...'):

            if 'df_vendas_final' in lista_keys_fora_do_session_state:

                st.session_state.df_vendas_final = gerar_df_vendas_final()

            if 'df_paxs_in' in lista_keys_fora_do_session_state:

                gerar_df_paxs_in()

df_agrupado = gerar_df_agrupado()

filtrar_ano = st.multiselect('Excluir Ano de Análise', sorted(df_agrupado['Ano'].unique().tolist()), default=None)

if len(filtrar_ano) > 0:

    df_agrupado = df_agrupado[~df_agrupado['Ano'].isin(filtrar_ano)]

df_filtrado = df_agrupado[pd.notna(df_agrupado['Setor'])]

setores = df_filtrado['Setor'].unique()

plotar_graficos_linha_por_setor(setores, df_agrupado)

df_vendas_mensal = df_filtrado.groupby(['Mes', 'Ano', 'Setor'], as_index=False)['Valor_Total'].sum()

plotar_graficos_barra_valor_total_por_setor(df_vendas_mensal)

df_vendas_anual = df_filtrado.groupby(['Ano', 'Setor'], as_index=False)['Valor_Total'].sum()

df_vendas_anual = df_vendas_anual.sort_values(by=['Setor', 'Ano'])

df_vendas_anual['Variacao_Anual'] = df_vendas_anual.groupby('Setor')['Valor_Total'].pct_change() * 100

plotar_grafico_barra_valor_total_por_setor_ano_resumo(df_vendas_anual)

df_agrupado['Paxs'] = df_agrupado['Paxs'].astype(float)

df_filtro_paxs = df_agrupado.loc[df_agrupado.groupby('Mes_Ano')['Paxs'].idxmax()]

plotar_grafico_fluxo_paxs(df_filtro_paxs)

df_agrupado['Valor_Total'] = df_agrupado['Valor_Total'].astype(float)

df_filtro_receita = df_agrupado.groupby('Mes_Ano').agg({'Valor_Total': 'sum', 'Paxs' : 'mean'}).reset_index()

df_filtro_receita['Ticket_Medio'] = df_filtro_receita['Valor_Total'] / df_filtro_receita['Paxs']

df_filtro_receita['Variacao_Percentual'] = df_filtro_receita['Valor_Total'].pct_change() * 100

plotar_grafico_ticket_medio(df_filtro_receita)
