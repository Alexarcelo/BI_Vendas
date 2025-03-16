import streamlit as st
import pandas as pd
import mysql.connector
import decimal
import gspread
from google.oauth2 import service_account
import numpy as np
from babel.numbers import format_currency
import plotly.express as px
import locale
from datetime import date
import sys
from pathlib import Path

def gerar_df_historico():

    puxar_aba_simples(st.session_state.id_gsheet_metas_vendas, 'BD - Historico', 'df_historico')

    tratar_colunas_numero_df(st.session_state.df_historico, st.session_state.lista_colunas_numero_df_historico)
    
    st.session_state.df_historico['Mes_Ano'] = pd.to_datetime(st.session_state.df_historico['Ano'].astype(str) + '-' + st.session_state.df_historico['Mes'].astype(str) + '-01').dt.to_period('M')

def gerar_df_ranking():

    request_select = '''SELECT * FROM vw_ranking_bi_vendas'''
    
    st.session_state.df_ranking = gerar_df_phoenix(st.session_state.base_luck, request_select)

    st.session_state.df_ranking['Data_Execucao'] = pd.to_datetime(st.session_state.df_ranking['Data_Execucao']).dt.date

    st.session_state.df_ranking['Ano'] = pd.to_datetime(st.session_state.df_ranking['Data_Execucao']).dt.year
    
    st.session_state.df_ranking['Mes'] = pd.to_datetime(st.session_state.df_ranking['Data_Execucao']).dt.month
    
    st.session_state.df_ranking['Mes_Ano'] = pd.to_datetime(st.session_state.df_ranking['Data_Execucao']).dt.to_period('M')
    
    st.session_state.df_ranking['Total Paxs'] = st.session_state.df_ranking['Total_ADT'] + st.session_state.df_ranking['Total_CHD'] / 2

def filtrar_periodo_dfs():

    df_vendas = st.session_state.df_vendas_final[(st.session_state.df_vendas_final['Ano'].isin(ano_selecao)) & 
                                                 (st.session_state.df_vendas_final['Mes'].isin(st.session_state.mes_selecao_valores))].reset_index(drop=True)

    df_paxs_in = st.session_state.df_paxs_in[(st.session_state.df_paxs_in['Ano'].isin(ano_selecao)) & 
                                             (st.session_state.df_paxs_in['Mes'].isin(st.session_state.mes_selecao_valores))].reset_index(drop=True)
    
    df_ranking = st.session_state.df_ranking[(st.session_state.df_ranking['Ano'].isin(ano_selecao)) & 
                                             (st.session_state.df_ranking['Mes'].isin(st.session_state.mes_selecao_valores))].reset_index(drop=True)
    
    if st.session_state.base_luck =='test_phoenix_joao_pessoa':

        df_historico = st.session_state.df_historico[(st.session_state.df_historico['Ano'].isin(ano_selecao)) & 
                                                     (st.session_state.df_historico['Mes'].isin(st.session_state.mes_selecao_valores))].reset_index(drop=True)

        return df_vendas, df_paxs_in, df_historico, df_ranking
    
    else:

        return df_vendas, df_paxs_in, df_ranking

def adicionar_historico_de_vendas(df_historico, df_vendas):

    df_historico = df_historico.rename(columns={'Data': 'Data_Venda', 'Paxs ADT': 'Total_Paxs'})

    df_vendas = pd.concat([df_vendas, df_historico[['Ano', 'Mes', 'Setor', 'Valor_Venda', 'Total_Paxs', 'Mes_Ano']]], ignore_index=True)

    return df_vendas, df_historico

def calculando_soma_total_paxs_paxs_desc(df_paxs_in, df_metas_setor, df_vendas_agrupado):

    total_paxs_in = df_paxs_in['Total_Paxs'].sum()

    if st.session_state.base_luck == 'test_phoenix_joao_pessoa':

        total_paxs_desc = df_metas_setor['Paxs_Desc'].sum()

    else:

        total_paxs_desc = 0

    df_vendas_agrupado['Total_Paxs'] = total_paxs_in + total_paxs_desc

    return df_vendas_agrupado

def gerar_df_vendas_agrupado(df_vendas, df_metas_setor, df_paxs_in):
    
    def calculando_ordenando_venda_liquida_reembolsos(df_vendas_agrupado):

        df_vendas_agrupado['Venda_Filtrada'] = df_vendas_agrupado['Valor_Venda'] - df_vendas_agrupado['Valor_Reembolso'].fillna(0)

        df_vendas_agrupado = df_vendas_agrupado.sort_values(by='Venda_Filtrada', ascending=False)

        return df_vendas_agrupado

    df_vendas_agrupado = df_vendas.groupby(['Vendedor', 'Setor', 'Mes_Ano'], dropna=False, as_index=False).agg({'Valor_Venda': 'sum', 'Valor_Reembolso': 'sum', 'Desconto_Global_Ajustado': 'sum'})

    df_vendas_agrupado = calculando_soma_total_paxs_paxs_desc(df_paxs_in, df_metas_setor, df_vendas_agrupado)

    df_vendas_agrupado = calculando_ordenando_venda_liquida_reembolsos(df_vendas_agrupado)

    return df_vendas_agrupado

def gerar_df_vendas_agrupado_setor(df_vendas_agrupado):

    df_vendas_agrupado_setor = df_vendas_agrupado.groupby('Setor', as_index=False).agg({'Venda_Filtrada': 'sum','Total_Paxs': 'mean'})

    df_vendas_agrupado_setor = df_vendas_agrupado_setor.sort_values(by='Venda_Filtrada', ascending=False)

    df_vendas_agrupado_setor = df_vendas_agrupado_setor[pd.notna(df_vendas_agrupado_setor['Setor'])]

    df_vendas_agrupado_setor['Ticket_Medio'] = df_vendas_agrupado_setor['Venda_Filtrada'] / df_vendas_agrupado_setor['Total_Paxs']

    return df_vendas_agrupado_setor

def formatar_moeda(valor):

    return format_currency(valor, 'BRL', locale='pt_BR')

def gerar_grafico_valor_total_setor(df_vendas_agrupado_setor):

    fig = px.bar(
        x=df_vendas_agrupado_setor['Setor'], 
        y=df_vendas_agrupado_setor['Venda_Filtrada'],
        color=df_vendas_agrupado_setor['Setor'],  
        title='Valor Total por Setor',
        labels={'Venda_Filtrada': 'Valor Total', 'Setor': 'Setores'},
        text=df_vendas_agrupado_setor['Venda_Filtrada'].apply(formatar_moeda)
        )

    ticket_medio_line = px.line(
        x=df_vendas_agrupado_setor['Setor'],
        y=df_vendas_agrupado_setor['Ticket_Medio'],
        line_shape='spline'
        )
    
    fig.add_trace(ticket_medio_line.data[0])
    fig.data[-1].name = 'Ticket Medio'
    fig.data[-1].line.color = 'orange'
    fig.data[-1].line.width = 1  # Diminuindo a espessura do spline
    fig.data[-1].yaxis = 'y2'
    fig.data[-1].mode = 'lines+markers+text'
    fig.data[-1].marker = dict(size=8)
    fig.data[-1].text = df_vendas_agrupado_setor['Ticket_Medio'].apply(formatar_moeda)

    fig.data[-1].textposition = 'top center'

    fig.update_traces(
        textposition='outside',
        textfont=dict(size=10),
        selector=dict(type='bar')
    )
    fig.update_layout(
        yaxis_title='Valor Total',
        xaxis_title='Setores',
        yaxis2=dict(title='Ticket Medio', overlaying='y', side='right', showgrid=False, zeroline=False, range=[-500, 400]  )
    )

    return fig

def gerar_df_vendas_agrupado_mes_setor(df_vendas, df_metas_setor):

    df_vendas_agrupado_mes = df_vendas.groupby(['Mes_Ano', 'Vendedor', 'Setor'], dropna=False).agg({'Valor_Venda': 'sum', 'Valor_Reembolso': 'sum', 'Desconto_Global_Ajustado': 'sum'}).reset_index()

    df_vendas_agrupado_mes = calculando_soma_total_paxs_paxs_desc(df_paxs_in, df_metas_setor, df_vendas_agrupado_mes)

    df_vendas_agrupado_mes['Venda_Filtrada'] = df_vendas_agrupado_mes['Valor_Venda'].fillna(0) - df_vendas_agrupado_mes['Valor_Reembolso'].fillna(0)

    df_vendas_agrupado_mes_setor = df_vendas_agrupado_mes.groupby(['Mes_Ano', 'Setor'], as_index=False).agg({'Venda_Filtrada': 'sum'})

    df_vendas_agrupado_mes_setor['Mes_Ano'] = df_vendas_agrupado_mes_setor['Mes_Ano'].dt.strftime('%B %Y').replace(st.session_state.meses_ingles_portugues, regex=True)

    return df_vendas_agrupado_mes_setor

def plotar_graficos_pizza_vendas_setor_mes(df_vendas_group_mes_setor, colunas):

    for i, mes in enumerate(df_vendas_group_mes_setor['Mes_Ano'].unique()):

        df_mes = df_vendas_group_mes_setor[df_vendas_group_mes_setor['Mes_Ano'] == mes]
        
        fig = px.pie(
            df_mes,
            names='Setor',
            values='Venda_Filtrada',
            title=f'{mes}',
            labels={'Venda_Filtrada': 'Valor Filtrado', 'Setor': 'Setores'},
            hole=0.3,  
        )
        colunas[i % 2].plotly_chart(fig)

def gerar_rankings_filtrados_geral(df_ranking, passeios_incluidos):

    def gerar_ranking_filtrado_combo_setores(df_ranking):

        ranking_filtrado_combo = df_ranking.groupby(['Setor', 'Servico', 'Mes_Ano'], as_index=False)['Total Paxs'].sum()

        ranking_filtrado_combo_setores = ranking_filtrado_combo[pd.notna(ranking_filtrado_combo['Setor'])]

        return ranking_filtrado_combo_setores
    
    def gerar_ranking_filtrado(df_ranking):

        ranking_filtrado = df_ranking[df_ranking['Servico'].isin(passeios_incluidos)]

        ranking_filtrado = ranking_filtrado.groupby(['Setor', 'Servico', 'Mes_Ano'], as_index=False)['Total Paxs'].sum()

        return ranking_filtrado
    
    if st.session_state.base_luck == 'test_phoenix_joao_pessoa':

        df_ranking['Servico'] = df_ranking['Servico'].replace({'EMBARCAÇAO - CATAMARÃ DO FORRÓ ': 'CATAMARÃ DO FORRÓ', 'INGRESSO - BY NIGHT ': 'BY NIGHT PARAHYBA OXENTE '}) 

    ranking_filtrado_combo_setores = gerar_ranking_filtrado_combo_setores(df_ranking)

    ranking_filtrado = gerar_ranking_filtrado(df_ranking)

    ranking_filtrado_setores = ranking_filtrado[pd.notna(ranking_filtrado['Setor'])]

    ranking_filtrado_geral = ranking_filtrado.groupby(['Servico', 'Mes_Ano'], as_index=False)['Total Paxs'].sum()

    mes_ranking_geral = ranking_filtrado_geral['Mes_Ano'].dt.strftime('%B %Y').replace(st.session_state.meses_ingles_portugues, regex=True).unique()

    return ranking_filtrado_combo_setores, ranking_filtrado_setores, ranking_filtrado_geral, mes_ranking_geral

def plotar_graficos_pizza_desempenho_passeios_geral(mes_ranking_geral, ranking_filtrado_geral, colunas):

    i = 0

    for mes_geral in mes_ranking_geral:

        df_ranking_geral_chart = ranking_filtrado_geral[(ranking_filtrado_geral['Mes_Ano'].dt.strftime('%B %Y').replace(st.session_state.meses_ingles_portugues, regex=True) == mes_geral)]

        if not df_ranking_geral_chart.empty:

            fig_1 = px.pie(
                df_ranking_geral_chart,
                names='Servico',
                values='Total Paxs',
                title=f'Desempenho Passeios Geral - {mes_geral}',
                labels={'Total Paxs': 'Total Paxs', 'Passeio': 'Servico'},
                hole=0.3,
                category_orders={'Servico': sorted(df_ranking_geral_chart['Servico'].unique())}
                )
            
            colunas[i % 2].plotly_chart(fig_1)

            i+=1

def plotar_graficos_pizza_desempenho_passeios_por_setor(ranking_filtrado_setores, ranking_filtrado_combo_setores):

    mes_ranking = ranking_filtrado_setores['Mes_Ano'].dt.strftime('%B %Y').replace(st.session_state.meses_ingles_portugues, regex=True).unique()

    setor_ranking = ranking_filtrado_setores['Setor'].unique()

    for mes_ in mes_ranking:

        for setor_ in setor_ranking:

            df_ranking_chart = ranking_filtrado_setores[(ranking_filtrado_setores['Mes_Ano'].dt.strftime('%B %Y').replace(st.session_state.meses_ingles_portugues, regex=True) == mes_) & 
                                                        (ranking_filtrado_setores['Setor'] == setor_)]
            
            df_ranking_combos = ranking_filtrado_combo_setores[(ranking_filtrado_combo_setores['Mes_Ano'].dt.strftime('%B %Y').replace(st.session_state.meses_ingles_portugues, regex=True) == mes_) & 
                                                               (ranking_filtrado_combo_setores['Setor'] == setor_)]

            df_ranking_combos['Combo'] = df_ranking_combos['Servico'].apply(lambda x: 'MIX LUCK' if x in st.session_state.combo_luck else 'MIX OUTROS')

            df_combos_contador = df_ranking_combos.groupby('Combo', as_index=False)['Total Paxs'].sum()
            
            if not df_ranking_chart.empty:

                fig_2 = px.pie(
                    df_ranking_chart,
                    names='Servico',
                    values='Total Paxs',
                    title=f'Desempenho Principais Passeios - {mes_} - Setor: {setor_}',
                    labels={'Total Paxs': 'Total Paxs', 'Passeio': 'Servico'},
                    hole=0.3,
                    category_orders={'Servico': sorted(df_ranking_chart['Servico'].unique())}
                    )
                
            if not df_combos_contador.empty:
                
                fig_3 = px.pie(
                    df_combos_contador,
                    names='Combo',
                    values='Total Paxs',
                    title=f'Desempenho Mix Luck | Outros Passeios - {mes_} - Setor: {setor_}',
                    labels={'Total Paxs': 'Total Paxs', 'Combo': 'Mix de Passeios'},
                    category_orders={'Servico': sorted(df_combos_contador['Combo'].unique())}
                    )
                
                with st.container():

                    colunas_01 = st.columns(2)

                    with colunas_01[0]:

                        st.plotly_chart(fig_2, use_container_width=True)

                    with colunas_01[1]:

                        st.plotly_chart(fig_3, use_container_width=True)

sys.path.append(str(Path(__file__).resolve().parent))

from Vendas_Gerais import puxar_aba_simples, tratar_colunas_numero_df, gerar_df_phoenix, puxar_df_config, gerar_df_metas, gerar_df_vendas_final, gerar_df_paxs_in, ajustar_desconto_global

st.title('Gerencial - Mês a Mês')

st.divider()

if st.session_state.base_luck == 'test_phoenix_joao_pessoa':

    lista_keys_fora_do_session_state = [item for item in ['df_reembolsos', 'df_metas', 'df_config', 'df_historico', 'df_vendas_final', 'anos_disponiveis', 'df_ranking', 'df_paxs_in'] 
                                        if item not in st.session_state]

    if len(lista_keys_fora_do_session_state)>0:

        with st.spinner('Puxando dados do Google Drive...'):

            if 'df_metas' in lista_keys_fora_do_session_state:

                gerar_df_metas()

            if 'df_config' in lista_keys_fora_do_session_state:

                puxar_df_config()

            if 'df_historico' in lista_keys_fora_do_session_state:

                gerar_df_historico()

        with st.spinner('Puxando dados do Phoenix...'):

            if 'df_vendas_final' in lista_keys_fora_do_session_state:

                st.session_state.df_vendas_final = gerar_df_vendas_final()

            if 'anos_disponiveis' in lista_keys_fora_do_session_state:

                st.session_state.anos_disponiveis = st.session_state.df_vendas_final['Ano'].unique().tolist()

            if 'df_ranking' in lista_keys_fora_do_session_state:

                gerar_df_ranking()

            if 'df_paxs_in' in lista_keys_fora_do_session_state:

                gerar_df_paxs_in()

elif st.session_state.base_luck in ['test_phoenix_natal', 'test_phoenix_salvador']:

    lista_keys_fora_do_session_state = [item for item in ['df_reembolsos', 'df_metas', 'df_config', 'df_vendas_final', 'anos_disponiveis', 'df_ranking', 'df_paxs_in'] 
                                        if item not in st.session_state]

    if len(lista_keys_fora_do_session_state)>0:

        with st.spinner('Puxando reembolsos, metas de vendedores, metas de setores, configurações, histórico...'):

            if 'df_metas' in lista_keys_fora_do_session_state:

                gerar_df_metas()

            if 'df_config' in lista_keys_fora_do_session_state:

                puxar_df_config()

        with st.spinner('Puxando vendas, ranking, guias IN e paxs IN do Phoenix...'):

            if 'df_vendas_final' in lista_keys_fora_do_session_state:

                st.session_state.df_vendas_final = gerar_df_vendas_final()

            if 'anos_disponiveis' in lista_keys_fora_do_session_state:

                st.session_state.anos_disponiveis = st.session_state.df_vendas_final['Ano'].unique().tolist()

            if 'df_ranking' in lista_keys_fora_do_session_state:

                gerar_df_ranking()

            if 'df_paxs_in' in lista_keys_fora_do_session_state:

                gerar_df_paxs_in()

col1, col2 = st.columns([2, 4])

with col1:

    ano_selecao = st.multiselect('Selecione o Ano:', st.session_state.anos_disponiveis, default=[date.today().year], key='ano_selecao')

with col2:

    mes_selecao = st.multiselect('Selecione o Mês:', st.session_state.meses_disponiveis.keys(), default=list(st.session_state.meses_disponiveis.keys())[:date.today().month], key='mes_selecao')

    st.session_state.mes_selecao_valores = [st.session_state.meses_disponiveis[mes] for mes in mes_selecao]

if len(ano_selecao)>0 and len(mes_selecao)>0:

    if st.session_state.base_luck == 'test_phoenix_joao_pessoa':

        df_vendas, df_paxs_in, df_historico, df_ranking = filtrar_periodo_dfs()

        df_vendas, df_historico = adicionar_historico_de_vendas(df_historico, df_vendas)

    else:

        df_vendas, df_paxs_in, df_ranking = filtrar_periodo_dfs()

    df_vendas = ajustar_desconto_global(df_vendas)

    df_vendas_agrupado = gerar_df_vendas_agrupado(df_vendas, st.session_state.df_metas, df_paxs_in)

    df_vendas_agrupado_setor = gerar_df_vendas_agrupado_setor(df_vendas_agrupado)

    fig = gerar_grafico_valor_total_setor(df_vendas_agrupado_setor)

    st.plotly_chart(fig)

    df_vendas_agrupado_mes_setor = gerar_df_vendas_agrupado_mes_setor(df_vendas, st.session_state.df_metas)

    st.title('Fatias de Vendas por Setor')

    colunas = st.columns(2)

    plotar_graficos_pizza_vendas_setor_mes(df_vendas_agrupado_mes_setor, colunas)

    st.title('Desempenho Passeios Geral')

    ranking_filtrado_combo_setores, ranking_filtrado_setores, ranking_filtrado_geral, mes_ranking_geral = gerar_rankings_filtrados_geral(df_ranking, st.session_state.passeios_incluidos)

    colunas = st.columns(2)

    plotar_graficos_pizza_desempenho_passeios_geral(mes_ranking_geral, ranking_filtrado_geral, colunas)

    st.title('Desempenho Passeios Por Setor')

    plotar_graficos_pizza_desempenho_passeios_por_setor(ranking_filtrado_setores, ranking_filtrado_combo_setores)

else:

    st.warning('Selecione pelo menos um ano e um mês para a análise')
