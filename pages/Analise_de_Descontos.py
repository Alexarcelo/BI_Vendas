import streamlit as st
import pandas as pd
import mysql.connector
import decimal
import gspread
from google.oauth2 import service_account
from babel.numbers import format_currency
import sys
from pathlib import Path

def gerar_df_descontos():

    df_descontos = st.session_state.df_vendas_final.copy()

    df_descontos['Mes_Nome'] = df_descontos['Mes_Ano'].dt.strftime('%B')

    df_descontos['Mes_Nome'] = df_descontos['Mes_Nome'].replace(st.session_state.meses_ingles_portugues)

    df_descontos['Ano'] = df_descontos['Mes_Ano'].dt.year

    df_descontos = df_descontos[pd.notna(df_descontos['Setor'])]

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

    if st.session_state.base_luck == 'test_phoenix_joao_pessoa':

        df_descontos1.loc[df_descontos1['Servico']=='EXTRA', 'Desconto_Global'] = 0

    df_agrupado_descontos = df_descontos1.groupby(['Reserva', 'Vendedor']).agg({'Mes_Ano': 'first', 'Desconto_Global': 'min', 'Valor_Venda': 'sum', 'Servico': 'count', 'Total Paxs': 'max', 
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

sys.path.append(str(Path(__file__).resolve().parent.parent))

from Vendas_Gerais import puxar_df_config, gerar_df_metas, gerar_df_vendas_final

st.title('Descontos')

st.write(f'_Total de Vendas Considerando apenas as Vendas Realizadas - Não considerando os reembolsos_')

st.divider()

lista_keys_fora_do_session_state = [item for item in ['df_config', 'df_metas', 'df_vendas_final'] if item not in st.session_state]

if len(lista_keys_fora_do_session_state)>0:

    with st.spinner('Puxando dados do Google Drive...'):

        if 'df_config' in lista_keys_fora_do_session_state:

            puxar_df_config()

        if 'df_metas' in lista_keys_fora_do_session_state:

            gerar_df_metas()

    with st.spinner('Puxando dados do Phoenix...'):

        if 'df_vendas_final' in lista_keys_fora_do_session_state:

            st.session_state.df_vendas_final = gerar_df_vendas_final()

df_descontos = gerar_df_descontos()

seleciona_ano, seleciona_mes, seleciona_vendedor = colher_selecao_ano_mes_vendedor(df_descontos)

if len(seleciona_ano)>0 and len(seleciona_mes)>0 and len(seleciona_vendedor)>0:

    df_agrupado_descontos = gerar_df_agrupado_descontos(df_descontos, seleciona_ano, seleciona_mes, seleciona_vendedor)

    df_filtrado_print = gerar_df_filtrado_print(df_agrupado_descontos)

    st.dataframe(
        df_filtrado_print[
            [
                'Reserva', 
                'Valor_Venda', 
                'Desconto_Global', 
                '% Desconto', 
                'Servico', 
                'Total Paxs', 
                'TM Reserva'
            ]
        ].rename(
            columns={
                'Reserva': 'Reserva', 
                'Valor_Venda': 'Total Venda', 
                'Desconto_Global': 'Total Desconto', 
                'Servico': 'Passeios Vendidos'
            }
        ), 
        hide_index=True, 
        use_container_width=True
    )

    df_individual = gerar_df_individual(df_agrupado_descontos)

    st.dataframe(
        df_individual, 
        hide_index=True, 
        use_container_width=True
    )

else:

    st.warning('Selecione pelo menos um Ano, Mês e Vendedor')
