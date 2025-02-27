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

def gerar_df_historico_vendedor():

    puxar_aba_simples(st.session_state.id_gsheet_metas_vendas, 'BD - Historico_Vendedor', 'df_historico_vendedor')

    tratar_colunas_numero_df(st.session_state.df_historico_vendedor, st.session_state.lista_colunas_numero_df_historico_vendedor)

    st.session_state.df_historico_vendedor['Mes_Ano'] = pd.to_datetime(st.session_state.df_historico_vendedor['Ano'].astype(str) + '-' + 
                                                                       st.session_state.df_historico_vendedor['Mes'].astype(str) + '-01').dt.to_period('M')                                                               

    st.session_state.df_historico_vendedor['Setor'] = st.session_state.df_historico_vendedor['Vendedor'].str.split(' - ').str[1]

    st.session_state.df_historico_vendedor['Ticket_Medio'] = st.session_state.df_historico_vendedor['Valor'] / st.session_state.df_historico_vendedor['Paxs_Total']

    st.session_state.df_historico_vendedor['Venda_Esperada'] = st.session_state.df_historico_vendedor['Paxs_Total'] * st.session_state.df_historico_vendedor['Meta']

def gerar_df_ranking():

    request_select = '''SELECT * FROM vw_ranking_bi_vendas'''
    
    st.session_state.df_ranking = gerar_df_phoenix(st.session_state.base_luck, request_select)

    st.session_state.df_ranking['Data_Execucao'] = pd.to_datetime(st.session_state.df_ranking['Data_Execucao']).dt.date

    st.session_state.df_ranking['Ano'] = pd.to_datetime(st.session_state.df_ranking['Data_Execucao']).dt.year
    
    st.session_state.df_ranking['Mes'] = pd.to_datetime(st.session_state.df_ranking['Data_Execucao']).dt.month
    
    st.session_state.df_ranking['Mes_Ano'] = pd.to_datetime(st.session_state.df_ranking['Data_Execucao']).dt.to_period('M')

    if st.session_state.base_luck == 'test_phoenix_joao_pessoa':

        st.session_state.df_ranking['Setor'] = st.session_state.df_ranking['Vendedor'].str.split(' - ').str[1].replace({'OPERACIONAL':'LOGISTICA', 'BASE AEROPORTO ': 'LOGISTICA', 
                                                                                                                        'BASE AEROPORTO': 'LOGISTICA', 'COORD. ESCALA': 'LOGISTICA', 
                                                                                                                        'KUARA/MANSEAR': 'LOGISTICA'})
    
    st.session_state.df_ranking['Total Paxs'] = st.session_state.df_ranking['Total_ADT'] + st.session_state.df_ranking['Total_CHD'] / 2

def gerar_df_paxs_mes():

    df_paxs_in = st.session_state.df_paxs_in.groupby(['Mes_Ano'], as_index=False)['Total_Paxs'].sum()

    df_paxs_mes = pd.merge(df_paxs_in, st.session_state.df_metas, on=['Mes_Ano'], how='left')

    if st.session_state.base_luck == 'test_phoenix_joao_pessoa':

        df_paxs_mes['Paxs_IN_Mensal'] = df_paxs_mes['Total_Paxs'].fillna(0) + df_paxs_mes['Paxs_Desc'].fillna(0)

    elif st.session_state.base_luck == 'test_phoenix_natal':

        df_paxs_mes['Paxs_IN_Mensal'] = df_paxs_mes['Total_Paxs'].fillna(0)

    return df_paxs_mes

def gerar_df_vendas(df_paxs_mes, df_guias_in, df_ocupacao_hoteis=None):

    def gerar_df_vendas_agrupado():

        df_vendas = st.session_state.df_vendas_final.copy()

        df_vendas = df_vendas[pd.notna(df_vendas['Setor'])]

        if st.session_state.base_luck == 'test_phoenix_joao_pessoa':

            df_vendas['Desconto_Global_Ajustado'] = df_vendas.apply(lambda row: row['Desconto_Global_Por_Servico'] if pd.notna(row['Desconto_Global_Por_Servico']) and 
                                                                    row['Desconto_Global_Por_Servico'] < 1000 and row['Nome_Servico'] != 'EXTRA' else 0, axis=1)

        df_vendas = df_vendas.groupby(['Vendedor', 'Mes_Ano'], dropna=False, as_index=False).agg({'Valor_Venda': 'sum', 'Valor_Reembolso': 'sum', 'Desconto_Global_Por_Servico': 'sum', 'Meta': 'first', 
                                                                                                  'Ano': 'mean', 'Mes': 'mean', 'Setor': 'first'})
        
        return df_vendas
    
    def adicionar_paxs_real_paxs_in_meta_vendedor(df_paxs_mes, df_guias_in, df_vendas, df_ocupacao_hoteis=None):

        df_vendas = pd.merge(df_vendas, df_paxs_mes[['Paxs_IN_Mensal', 'Mes_Ano']], on='Mes_Ano', how='left')

        df_guias_in = df_guias_in.rename(columns={'Guia': 'Vendedor', 'Total_Paxs': 'Paxs_IN_Individual'})

        df_vendas = pd.merge(df_vendas, df_guias_in[['Vendedor', 'Mes_Ano','Paxs_IN_Individual']], on=['Vendedor', 'Mes_Ano'], how='left')

        if not df_ocupacao_hoteis is None:

            df_ocupacao_hoteis = df_ocupacao_hoteis.groupby(['Mes_Ano', 'Vendedor'], as_index=False)['Paxs Hotel'].sum()

            df_vendas = pd.merge(df_vendas, df_ocupacao_hoteis[['Mes_Ano', 'Vendedor', 'Paxs Hotel']], on=['Mes_Ano', 'Vendedor'], how='left')

        df_vendas = pd.merge(df_vendas, st.session_state.df_metas_vendedor[['Vendedor', 'Mes_Ano', 'Meta_Mes']], on=['Vendedor', 'Mes_Ano'], how='left')

        return df_vendas
    
    def ajustar_colunas_meta_mes_total_paxs_venda_filtrada_ticket_medio_venda_esperada(df_vendas):

        df_vendas['Meta_Mes'] = df_vendas['Meta_Mes'].fillna(df_vendas['Meta'])

        df_vendas['Paxs_IN_Individual'] = df_vendas['Paxs_IN_Individual'].fillna(0)

        df_vendas.loc[~df_vendas['Setor'].isin(['Guia', 'Transferista']), 'Paxs_IN_Individual'] = df_vendas['Paxs_IN_Mensal']

        if st.session_state.base_luck == 'test_phoenix_natal':

            df_vendas['Paxs Hotel'] = df_vendas['Paxs Hotel'].fillna(0)

            df_vendas.loc[df_vendas['Setor']=='Desks', 'Paxs_IN_Individual'] = df_vendas['Paxs Hotel']

        df_vendas['Venda_Filtrada'] = df_vendas['Valor_Venda'].fillna(0) - df_vendas['Valor_Reembolso'].fillna(0)

        df_vendas['Ticket_Medio'] = df_vendas['Venda_Filtrada'] / df_vendas['Paxs_IN_Individual']

        df_vendas['Venda_Esperada'] = df_vendas['Paxs_IN_Individual'] * df_vendas['Meta_Mes']

        return df_vendas
    
    df_vendas = gerar_df_vendas_agrupado()

    if st.session_state.base_luck == 'test_phoenix_natal':

        df_vendas = adicionar_paxs_real_paxs_in_meta_vendedor(df_paxs_mes, df_guias_in, df_vendas, df_ocupacao_hoteis)

    elif st.session_state.base_luck == 'test_phoenix_joao_pessoa':

        df_vendas = adicionar_paxs_real_paxs_in_meta_vendedor(df_paxs_mes, df_guias_in, df_vendas)

    df_vendas = ajustar_colunas_meta_mes_total_paxs_venda_filtrada_ticket_medio_venda_esperada(df_vendas)

    return df_vendas

def concatenar_vendas_com_historico_vendedor(df_vendas):

    df_phoenix_vendedor = df_vendas[['Vendedor', 'Setor', 'Venda_Filtrada', 'Meta_Mes', 'Paxs_IN_Individual', 'Mes_Ano', 'Ticket_Medio', 'Venda_Esperada']]

    if st.session_state.base_luck == 'test_phoenix_joao_pessoa':

        df_historico_vendedor = st.session_state.df_historico_vendedor[(st.session_state.df_historico_vendedor['Mes_Ano'] != '2024-04')]\
            [['Vendedor', 'Setor', 'Valor', 'Meta', 'Paxs_Total', 'Mes_Ano', 'Ticket_Medio', 'Venda_Esperada']]

        df_historico_vendedor = df_historico_vendedor.rename(columns={'Valor': 'Venda_Filtrada', 'Meta': 'Meta_Mes', 'Paxs_Total': 'Paxs_IN_Individual'})

        df_geral_vendedor_1 = pd.concat([df_historico_vendedor, df_phoenix_vendedor], ignore_index=True)

        return df_geral_vendedor_1
    
    elif st.session_state.base_luck == 'test_phoenix_natal':

        return df_phoenix_vendedor

def agrupar_ajustar_colunas_df_geral_vendedor(df_geral_vendedor_1):

    df_geral_vendedor = df_geral_vendedor_1.groupby(['Vendedor', 'Mes_Ano'], as_index=False).agg({'Setor': 'first', 'Venda_Filtrada': 'sum', 'Meta_Mes': 'min', 'Paxs_IN_Individual': 'min', 
                                                                                                  'Ticket_Medio': 'sum', 'Venda_Esperada': 'min'})

    df_geral_vendedor['Ticket_Medio'] = np.where(df_geral_vendedor['Paxs_IN_Individual']!=0, df_geral_vendedor['Venda_Filtrada'] / df_geral_vendedor['Paxs_IN_Individual'], 0)

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

def colher_ano_setor_vendedor_selecao(col1, col2, tipo_analise):

    if tipo_analise=='Historico por Vendedor':

        lista_anos = st.session_state.df_geral_vendedor['Ano'].unique().tolist()

        with col1:

            ano_selecao = st.multiselect('Selecione o Ano:', options=lista_anos, default=[], key='vend_0001')

        with col2:

            setor_selecao = st.multiselect('Selecione o Setor:', options=sorted(st.session_state.df_geral_vendedor['Setor'].unique().tolist()), default=[], key='vend_0002')

        return ano_selecao, setor_selecao
    
    elif tipo_analise=='Acompanhamento Anual - Vendedores':

        lista_anos = st.session_state.df_geral_vendedor['Ano'].unique().tolist()

        ano_atual = date.today().year

        df_filtro_lista = st.session_state.df_geral_vendedor.groupby(['Vendedor'], as_index=False)['Venda_Filtrada'].sum()

        lista_vendedor = df_filtro_lista['Vendedor'].unique().tolist()

        top_vendedores = df_filtro_lista.nlargest(5, 'Venda_Filtrada')['Vendedor'].tolist()

        with col1:

            ano_selecao = st.multiselect('Selecione o Ano:', options=lista_anos, default=ano_atual, key='perf_0001')

        with col2:

            vendedor_selecao = st.multiselect('Selecione o Vendedor:', options=lista_vendedor, default=top_vendedores, key='perf_0002')

        return ano_selecao, vendedor_selecao

def plotar_graficos_acumulado_meta_e_vendedor(col1, col2, tipo_analise):

    def gerar_grafico_acumulado_meta_1(vendedor, df):
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

    if tipo_analise=='Historico por Vendedor':

        df_filtrado = st.session_state.df_geral_vendedor[(st.session_state.df_geral_vendedor['Ano'].isin(ano_selecao)) & (st.session_state.df_geral_vendedor['Setor'].isin(setor_selecao))]

        df_filtrado['Mes_Ano'] = df_filtrado['Mes_Ano'].dt.strftime('%m/%y')

        vendedores = df_filtrado['Vendedor'].unique()

        for vendedor in vendedores:

            with col1:

                fig_anual = gerar_grafico_acumulado_meta_1(vendedor, df_filtrado)

                st.plotly_chart(fig_anual)

            with col2:

                fig_mensal = gerar_grafico_vendedor(vendedor, df_filtrado)

                st.plotly_chart(fig_mensal)

    elif tipo_analise=='Acompanhamento Anual - Vendedores':

        df_filtrado = st.session_state.df_geral_vendedor[(st.session_state.df_geral_vendedor['Ano'].isin(ano_selecao)) & (st.session_state.df_geral_vendedor['Vendedor'].isin(vendedor_selecao))]

        df_filtrado['Mes_Ano'] = df_filtrado['Mes_Ano'].dt.strftime('%m/%y')

        vendedores = df_filtrado['Vendedor'].unique()

        for vendedor in vendedores:

            fig_mensal = gerar_grafico_vendedor(vendedor, df_filtrado)

            st.plotly_chart(fig_mensal)

def criar_df_ranking_df_vendas_graficos():

    df_ranking = st.session_state.df_ranking.copy()

    df_vendas = st.session_state.df_geral_vendedor.copy()

    df_vendas['Mes_Nome'] = df_vendas['Mes_Ano'].dt.strftime('%B')

    df_vendas['Mes_Nome'] = df_vendas['Mes_Nome'].replace(st.session_state.meses_ingles_portugues)

    df_ranking['Mes_Nome'] = df_ranking['Mes_Ano'].dt.strftime('%B')

    df_ranking['Mes_Nome'] = df_ranking['Mes_Nome'].replace(st.session_state.meses_ingles_portugues)

    return df_vendas, df_ranking

def colher_ano_mes_setor_selecao(df_vendas):

    lista_anos = df_vendas['Ano'].unique().tolist()

    lista_mes = [valor for valor in st.session_state.meses_disponiveis.keys() if valor in df_vendas['Mes_Nome'].unique()]

    with col1:

        ano_selecao = st.multiselect('Selecione o Ano:', options=lista_anos, default=[], key='met_001')

    with col2:

        mes_selecao = st.multiselect('Selecione o Mês', options=lista_mes, default=[], key='met_002')

    with col3:

        setor_selecao = st.multiselect('Selecione o Setor:', options=sorted(st.session_state.df_geral_vendedor['Setor'].unique().tolist()), default=[], key='met_003')

    return ano_selecao, mes_selecao, setor_selecao

def plotar_graficos_e_tabelas_meta_mes():

    def filtrar_df_ranking_vendas(df_vendas, ano_selecao, setor_selecao, mes_selecao):

        df_vendas_filtrado = df_vendas[(df_vendas['Ano'].isin(ano_selecao)) & (df_vendas['Setor'].isin(setor_selecao)) & (df_vendas['Mes_Nome'].isin(mes_selecao))]

        df_vendas_filtrado['Mes_Ano'] = df_vendas_filtrado['Mes_Ano'].dt.strftime('%m/%y')

        df_vendas_filtrado['Falta p/ Meta'] = df_vendas_filtrado['Venda_Esperada'] - df_vendas_filtrado['Venda_Filtrada'] 

        df_ranking_filtrado = df_ranking[(df_ranking['Ano'].isin(ano_selecao)) & (df_ranking['Setor'].isin(setor_selecao)) & (df_ranking['Mes_Nome'].isin(mes_selecao))]

        ranking_filtrado_combo = df_ranking_filtrado.groupby(['Vendedor', 'Servico', 'Mes_Nome', 'Ano'], as_index=False)['Total Paxs'].sum()

        return df_vendas_filtrado, ranking_filtrado_combo
    
    def gerar_grafico_acumulado_meta_2(vendedor, df):

        df_vendedor = df[df['Vendedor'] == vendedor]

        df_anual = df_vendedor.groupby('Mes_Nome').agg({'Venda_Filtrada': 'mean', 'Venda_Esperada': 'mean', 'Performance_Mes': 'mean'}).reset_index()
        
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=df_anual['Mes_Nome'],
            y=df_anual['Venda_Filtrada'],
            name='Venda Mes',
            marker=dict(color='rgb(4,124,108)'),
            text=df_anual['Venda_Filtrada'].apply(formatar_moeda),
            textposition='outside',
            textfont=dict(size=10),
            width=0.3,             # Define a largura das barras
        ))
        fig.add_trace(go.Bar(
            x=df_anual['Mes_Nome'],
            y=df_anual['Venda_Esperada'],
            name='Meta Mes',
            marker=dict(color='steelblue'),
            text=df_anual['Venda_Esperada'].apply(formatar_moeda),
            textposition='outside',
            textfont=dict(size=10, color='steelblue'),
            width=0.3,             # Define a largura das barras
        ))
        fig.update_layout(
            title=f"Vendas - {vendedor} | Performance - {df_anual['Performance_Mes'].loc[0] * 100:.2f}%",
            xaxis_title="Mes",
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

    def gerar_df_filtrado_print(df_vendas_filtrado):

        def formatar_condicional(row):
        
            if row['Falta p/ Meta'] < 0:

                return ['background-color: lightgreen'] * len(row)
            
            else:
                
                return [''] * len(row)

        df_filtrado_print = df_vendas_filtrado.copy()

        df_filtrado_print = df_filtrado_print[df_filtrado_print['Vendedor'] == vendedor]

        df_filtrado_print = df_filtrado_print.rename(columns={'Venda_Filtrada': 'Venda Filtrada', 'Venda_Esperada': 'Venda Esperada'})

        df_filtrado_print = df_filtrado_print[['Vendedor', 'Venda Filtrada', 'Venda Esperada', 'Falta p/ Meta']]

        df_filtrado_print = df_filtrado_print.style.apply(formatar_condicional, axis=1)

        df_filtrado_print = df_filtrado_print.format({'Venda Filtrada': formatar_moeda, 'Venda Esperada': formatar_moeda, 'Falta p/ Meta': formatar_moeda})

        return df_filtrado_print
    
    def gerar_df_ranking_print(ranking_filtrado_combo):
    
        df_ranking_print = ranking_filtrado_combo[ranking_filtrado_combo['Vendedor'] == vendedor]

        df_ranking_print = df_ranking_print[['Vendedor', 'Servico', 'Total Paxs']]

        df_ranking_print['Total Paxs'] = pd.to_numeric(df_ranking_print['Total Paxs'])

        df_ranking_print = df_ranking_print.sort_values(by='Total Paxs', ascending=False)

        return df_ranking_print

    df_vendas_filtrado, ranking_filtrado_combo = filtrar_df_ranking_vendas(df_vendas, ano_selecao, setor_selecao, mes_selecao)

    vendedores = df_vendas_filtrado['Vendedor'].unique()

    for vendedor in vendedores:

        col01, col02 = st.columns([4, 8])

        with col01:

            fig_anual = gerar_grafico_acumulado_meta_2(vendedor, df_vendas_filtrado)

            st.plotly_chart(fig_anual, use_container_width=True)

        with col02:

            df_filtrado_print = gerar_df_filtrado_print(df_vendas_filtrado)

            st.dataframe(df_filtrado_print, use_container_width=True, hide_index=True)

            df_ranking_print = gerar_df_ranking_print(ranking_filtrado_combo)

            st.dataframe(df_ranking_print, use_container_width=True, hide_index=True, height=355)

st.set_page_config(layout='wide')

sys.path.append(str(Path(__file__).resolve().parent.parent))

from Vendas_Gerais import puxar_aba_simples, tratar_colunas_numero_df, puxar_df_config, gerar_df_metas, gerar_df_metas_vendedor, gerar_df_ocupacao_hoteis, gerar_df_phoenix, gerar_df_vendas_final, \
    gerar_df_guias_in, gerar_df_paxs_in

if st.session_state.base_luck == 'test_phoenix_joao_pessoa':

    lista_keys_fora_do_session_state = [item for item in ['df_config', 'df_historico_vendedor', 'df_metas', 'df_metas_vendedor', 'df_vendas_final', 'df_ranking', 'df_guias_in', 'df_paxs_in'] 
                                        if item not in st.session_state]
    
    if len(lista_keys_fora_do_session_state)>0:

        with st.spinner('Puxando configurações, histórico vendedores, metas de vendedores e metas de setores...'):

            if 'df_config' in lista_keys_fora_do_session_state:

                puxar_df_config()

            if 'df_historico_vendedor' in lista_keys_fora_do_session_state:

                gerar_df_historico_vendedor()

            if 'df_metas' in lista_keys_fora_do_session_state:

                gerar_df_metas()

            if 'df_metas_vendedor' in lista_keys_fora_do_session_state:

                gerar_df_metas_vendedor()

        with st.spinner('Puxando vendas, ranking, guias IN e paxs IN do Phoenix...'):

            if 'df_vendas_final' in lista_keys_fora_do_session_state:

                st.session_state.df_vendas_final = gerar_df_vendas_final()

            if 'df_ranking' in lista_keys_fora_do_session_state:

                gerar_df_ranking()

            if 'df_guias_in' in lista_keys_fora_do_session_state:

                gerar_df_guias_in()

            if 'df_paxs_in' in lista_keys_fora_do_session_state:

                gerar_df_paxs_in()

elif st.session_state.base_luck == 'test_phoenix_natal':

    lista_keys_fora_do_session_state = [item for item in ['df_config', 'df_metas', 'df_metas_vendedor', 'df_ocupacao_hoteis', 'df_vendas_final', 'df_ranking', 'df_guias_in', 'df_paxs_in'] 
                                        if item not in st.session_state]
    
    if len(lista_keys_fora_do_session_state)>0:

        with st.spinner('Puxando configurações, metas de vendedores e metas de setores...'):

            if 'df_config' in lista_keys_fora_do_session_state:

                puxar_df_config()

            if 'df_metas' in lista_keys_fora_do_session_state:

                gerar_df_metas()

            if 'df_metas_vendedor' in lista_keys_fora_do_session_state:

                gerar_df_metas_vendedor()

            if 'df_ocupacao_hoteis' in lista_keys_fora_do_session_state:

                gerar_df_ocupacao_hoteis()

        with st.spinner('Puxando vendas, ranking, guias IN e paxs IN do Phoenix...'):

            if 'df_vendas_final' in lista_keys_fora_do_session_state:

                st.session_state.df_vendas_final = gerar_df_vendas_final()

            if 'df_ranking' in lista_keys_fora_do_session_state:

                gerar_df_ranking()

            if 'df_guias_in' in lista_keys_fora_do_session_state:

                gerar_df_guias_in()

            if 'df_paxs_in' in lista_keys_fora_do_session_state:

                gerar_df_paxs_in()

row_titulo = st.columns(1)

tipo_analise = st.radio('Análise', ['Acompanhamento Anual - Vendedores', 'Historico por Vendedor', 'Meta Mês'], index=None)

if tipo_analise:

    with row_titulo[0]:

        st.title(tipo_analise)

        st.divider()

else:

    st.warning('Escolha um tipo de análise')

if not 'df_geral_vendedor' in st.session_state:

    df_paxs_mes = gerar_df_paxs_mes()

    df_guias_in = st.session_state.df_guias_in.groupby(['Guia', 'Mes_Ano'], as_index=False)['Total_Paxs'].sum()

    if st.session_state.base_luck == 'test_phoenix_joao_pessoa':

        df_vendas = gerar_df_vendas(df_paxs_mes, df_guias_in)

    elif st.session_state.base_luck == 'test_phoenix_natal':

        df_vendas = gerar_df_vendas(df_paxs_mes, df_guias_in, st.session_state.df_ocupacao_hoteis)

    df_geral_vendedor_1 = concatenar_vendas_com_historico_vendedor(df_vendas)

    df_geral_vendedor = agrupar_ajustar_colunas_df_geral_vendedor(df_geral_vendedor_1)

    st.session_state.df_geral_vendedor = adicionar_performance_anual_acumulado_anual_meta_anual(df_geral_vendedor)

if tipo_analise=='Historico por Vendedor':

    col1, col2 = st.columns([4, 8])
    
    ano_selecao, setor_selecao = colher_ano_setor_vendedor_selecao(col1, col2, tipo_analise)

    if len(ano_selecao)>0 and len(setor_selecao)>0:

        plotar_graficos_acumulado_meta_e_vendedor(col1, col2, tipo_analise)

    else:

        st.warning('Precisa selecionar pelo menos um Ano e Setor')

elif tipo_analise=='Acompanhamento Anual - Vendedores':

    col1, col2 = st.columns([4, 8])

    ano_selecao, vendedor_selecao = colher_ano_setor_vendedor_selecao(col1, col2, tipo_analise)

    if len(ano_selecao)>0 and len(vendedor_selecao)>0:

        plotar_graficos_acumulado_meta_e_vendedor(col1, col2, tipo_analise)

    else:

        st.warning('Precisa selecionar pelo menos um Ano e Vendedor')

elif tipo_analise=='Meta Mês':

    col1, col2, col3 = st.columns([4, 4, 4])
    
    df_vendas, df_ranking = criar_df_ranking_df_vendas_graficos()

    ano_selecao, mes_selecao, setor_selecao = colher_ano_mes_setor_selecao(df_vendas)

    if len(ano_selecao)>0 and len(mes_selecao)>0 and len(setor_selecao)>0:

        plotar_graficos_e_tabelas_meta_mes()

    else:

        st.warning('Precisa selecionar pelo menos um Ano, Mês e Setor')
