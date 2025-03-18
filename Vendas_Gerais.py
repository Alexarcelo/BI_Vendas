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

def gerar_df_metas_vendedor():

    puxar_aba_simples(st.session_state.id_gsheet_metas_vendas, 'BD - Metas_Vendedor', 'df_metas_vendedor')

    tratar_colunas_numero_df(st.session_state.df_metas_vendedor, st.session_state.lista_colunas_numero_df_metas_vendedor)

    st.session_state.df_metas_vendedor['Mes_Ano'] = pd.to_datetime(st.session_state.df_metas_vendedor['Ano'].astype(str) + '-' + 
                                                                   st.session_state.df_metas_vendedor['Mes'].astype(str) + '-01').dt.to_period('M')

def gerar_df_metas_vendedor_setor():

    puxar_aba_simples(st.session_state.id_gsheet_metas_vendas, 'BD - Metas_Vendedor_Setor', 'df_metas_vendedor_setor')

    tratar_colunas_numero_df(st.session_state.df_metas_vendedor_setor, st.session_state.lista_colunas_numero_df_metas_vendedor)

    st.session_state.df_metas_vendedor_setor['Mes_Ano'] = pd.to_datetime(st.session_state.df_metas_vendedor_setor['Ano'].astype(str) + '-' + 
                                                                         st.session_state.df_metas_vendedor_setor['Mes'].astype(str) + '-01').dt.to_period('M')

def gerar_df_metas_vendedor_canal_vendas():

    puxar_aba_simples(st.session_state.id_gsheet_metas_vendas, 'BD - Metas_Vendedor_Canal_de_Vendas', 'df_metas_vendedor_canal_vendas')

    tratar_colunas_numero_df(st.session_state.df_metas_vendedor_canal_vendas, st.session_state.lista_colunas_numero_df_metas_vendedor)

    st.session_state.df_metas_vendedor_canal_vendas['Mes_Ano'] = pd.to_datetime(st.session_state.df_metas_vendedor_canal_vendas['Ano'].astype(str) + '-' + 
                                                                                st.session_state.df_metas_vendedor_canal_vendas['Mes'].astype(str) + '-01').dt.to_period('M')
    
def gerar_df_metas():

    puxar_aba_simples(st.session_state.id_gsheet_metas_vendas, 'BD - Metas', 'df_metas')

    tratar_colunas_numero_df(st.session_state.df_metas, st.session_state.df_metas.columns)

    st.session_state.df_metas['Mes_Ano'] = pd.to_datetime(st.session_state.df_metas['Ano'].astype(str) + '-' + st.session_state.df_metas['Mes'].astype(str) + '-01').dt.to_period('M')

def gerar_df_ocupacao_hoteis():

    puxar_aba_simples(st.session_state.id_gsheet_metas_vendas, 'BD - Ocupação Hoteis', 'df_ocupacao_hoteis')

    tratar_colunas_numero_df(st.session_state.df_ocupacao_hoteis, st.session_state.lista_colunas_numero_df_ocupacao_hoteis)

    st.session_state.df_ocupacao_hoteis['Mes_Ano'] = pd.to_datetime(st.session_state.df_ocupacao_hoteis['Ano'].astype(str) + '-' + 
                                                                    st.session_state.df_ocupacao_hoteis['Mes'].astype(str) + '-01').dt.to_period('M')

def gerar_df_custos_com_adicionais():

    puxar_aba_simples(st.session_state.id_gsheet_metas_vendas, 'Custos com Adicionais', 'df_custos_com_adicionais')

    tratar_colunas_numero_df(st.session_state.df_custos_com_adicionais, st.session_state.lista_colunas_numero_df_custos_com_adicionais)

def puxar_df_config():

    puxar_aba_simples(st.session_state.id_gsheet_metas_vendas, 'Configurações Vendas', 'df_config')

    tratar_colunas_numero_df(st.session_state.df_config, st.session_state.lista_colunas_numero_df_config)

    st.session_state.passeios_incluidos = st.session_state.df_config[st.session_state.df_config['Configuração']=='Passeios Gráfico Pizza']['Parâmetro'].tolist()

    st.session_state.combo_luck = st.session_state.df_config[st.session_state.df_config['Configuração']=='Passeios Combo Luck']['Parâmetro'].tolist()

    if st.session_state.base_luck == 'test_phoenix_natal':

        st.session_state.servicos_terceiros = st.session_state.df_config[st.session_state.df_config['Configuração']=='Serviços de Terceiros']['Parâmetro'].tolist()

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

def gerar_df_vendas_final():

    def gerar_df_vendas_phoenix():

        request_select = '''SELECT * FROM vw_bi_vendas'''

        st.session_state.df_vendas = gerar_df_phoenix(st.session_state.base_luck, request_select)

    def gerar_df_vendas_manuais():

        puxar_aba_simples(st.session_state.id_gsheet_metas_vendas, 'BD - Vendas Manuais', 'df_vendas_manuais')

        tratar_colunas_numero_df(st.session_state.df_vendas_manuais, st.session_state.lista_colunas_numero_df_vendas_manuais)

        tratar_colunas_data_df(st.session_state.df_vendas_manuais, st.session_state.lista_colunas_data_df_vendas_manuais)

    def ajustar_nomes_leticia_soraya(df_vendas):

        df_vendas['Vendedor'] = df_vendas['Vendedor'].replace('SORAYA - TRANSFERISTA', 'SORAYA - GUIA')

        df_vendas.loc[df_vendas['Vendedor']=='SORAYA - GUIA', 'Setor'] = 'Transferista'

        df_vendas.loc[(df_vendas['Vendedor']=='LETICIA - TRANSFERISTA') & (pd.to_datetime(df_vendas['Data_Venda']).dt.year>=2025), ['Vendedor', 'Setor']] = ['LETICIA - GUIA', 'Transferista']

        df_vendas.loc[(df_vendas['Vendedor']=='LETICIA - TRANSFERISTA') & (pd.to_datetime(df_vendas['Data_Venda']).dt.year<2025), ['Vendedor', 'Setor']] = ['LETICIA - PDV', 'Desks']

        return df_vendas

    def ajustar_pdvs_facebook(df_vendas):

        mask_ref = (df_vendas['Vendedor'].isin(['RAQUEL - PDV', 'VALERIA - PDV', 'ROBERTA - PDV', 'LETICIA - PDV'])) & (pd.to_datetime(df_vendas['Data_Venda']).dt.year<2025) & \
            (df_vendas['Canal_de_Vendas']=='Facebook')
        
        df_vendas.loc[mask_ref, 'Setor'] = 'Guia'

        df_vendas.loc[mask_ref, 'Vendedor'] = df_vendas.loc[mask_ref, 'Vendedor'].apply(lambda x: x.replace('- PDV', '- GUIA'))

        return df_vendas

    def ajustar_colunas_data_venda_mes_ano_total_paxs(df_vendas):

        df_vendas['Data_Venda'] = pd.to_datetime(df_vendas['Data_Venda']).dt.date

        df_vendas['Mes_Ano'] = pd.to_datetime(df_vendas['Data_Venda']).dt.to_period('M')

        df_vendas['Ano'] = pd.to_datetime(df_vendas['Data_Venda']).dt.year

        df_vendas['Mes'] = pd.to_datetime(df_vendas['Data_Venda']).dt.month

        df_vendas['Total Paxs'] = df_vendas['Total_ADT'].fillna(0) + df_vendas['Total_CHD'].fillna(0) / 2

        return df_vendas
    
    def criar_coluna_setor_definir_metas(df_vendas):

        if st.session_state.base_luck == 'test_phoenix_salvador':

            dict_setor = dict(zip(st.session_state.df_canal_de_vendas_setor['Canal de Vendas'], st.session_state.df_canal_de_vendas_setor['Setor']))

            df_vendas.loc[pd.notna(df_vendas['Canal_de_Vendas']), 'Setor'] = df_vendas['Canal_de_Vendas'].map(dict_setor)

        elif st.session_state.base_luck == 'test_phoenix_noronha':

            df_vendas.loc[
                (df_vendas['Vendedor'].isin(['LUCAS', 'Renato Apory'])) &
                (df_vendas['Mes_Ano'] <= pd.Period('2025-03', freq='M')),
                'Setor'
            ] = 'Transferista'

        df_metas_indexed = st.session_state.df_metas.set_index('Mes_Ano')

        df_vendas['Meta'] = df_vendas.apply(lambda row: df_metas_indexed.at[row['Mes_Ano'], row['Setor']] 
                                            if row['Setor'] in df_metas_indexed.columns and row['Mes_Ano'] in df_metas_indexed.index 
                                            else 0, axis=1)
        
        return df_vendas

    # Puxando as vendas do Phoenix

    gerar_df_vendas_phoenix()

    # Puxando as vendas lançadas manualmente na planilha

    gerar_df_vendas_manuais()

    df_vendas = pd.concat([st.session_state.df_vendas, st.session_state.df_vendas_manuais], ignore_index=True)

    if st.session_state.base_luck == 'test_phoenix_joao_pessoa':

        # Ajustando nomes de letícia e soraya pra identificar o setor correto

        df_vendas = ajustar_nomes_leticia_soraya(df_vendas)

        # Identificando como guia Raquel, Valeria, Roberta e Letícia quando o canal de vendas é Facebook e o ano é antes de 2025

        df_vendas = ajustar_pdvs_facebook(df_vendas)

        # Ajustando formato de Data_Venda, criando coluna Mes_Ano e criando coluna Total Paxs

        df_vendas = ajustar_colunas_data_venda_mes_ano_total_paxs(df_vendas)

        # Criando coluna setor, identificando pessoal da logistica e colocando a meta p/ cada setor

        df_vendas = criar_coluna_setor_definir_metas(df_vendas)

    else:

        # Ajustando formato de Data_Venda, criando coluna Mes_Ano e criando coluna Total Paxs

        df_vendas = ajustar_colunas_data_venda_mes_ano_total_paxs(df_vendas)

        # Inserindo a meta de cada setor

        df_vendas = criar_coluna_setor_definir_metas(df_vendas)

    return df_vendas

def gerar_df_guias_in():

    request_select = '''SELECT * FROM vw_guias_in'''
    
    st.session_state.df_guias_in = gerar_df_phoenix(st.session_state.base_luck, request_select)

    st.session_state.df_guias_in['Total_Paxs'] = st.session_state.df_guias_in['Total_ADT'].fillna(0) + (st.session_state.df_guias_in['Total_CHD'].fillna(0) / 2)

    st.session_state.df_guias_in['Data da Escala'] = pd.to_datetime(st.session_state.df_guias_in['Data da Escala']).dt.date

    st.session_state.df_guias_in['Mes_Ano'] = pd.to_datetime(st.session_state.df_guias_in['Data da Escala']).dt.to_period('M')

    if st.session_state.base_luck == 'test_phoenix_joao_pessoa':

        substituicao = {'RAQUEL - PDV': 'RAQUEL - GUIA', 'VALERIA - PDV': 'VALERIA - GUIA', 'ROBERTA - PDV': 'ROBERTA - GUIA', 'LETICIA - TRANSFERISTA': 'LETICIA - GUIA', 
                        'SORAYA - BASE AEROPORTO ': 'SORAYA - GUIA', 'SORAYA - TRANSFERISTA': 'SORAYA - GUIA'}

        st.session_state.df_guias_in['Guia'] = st.session_state.df_guias_in['Guia'].replace(substituicao)

def gerar_df_paxs_in():

    request_select = '''SELECT * FROM vw_paxs_in'''
    
    st.session_state.df_paxs_in = gerar_df_phoenix(st.session_state.base_luck, request_select)

    st.session_state.df_paxs_in['Data_Execucao'] = pd.to_datetime(st.session_state.df_paxs_in['Data_Execucao']).dt.date
    
    st.session_state.df_paxs_in['Ano'] = pd.to_datetime(st.session_state.df_paxs_in['Data_Execucao']).dt.year
    
    st.session_state.df_paxs_in['Mes'] = pd.to_datetime(st.session_state.df_paxs_in['Data_Execucao']).dt.month
    
    st.session_state.df_paxs_in['Mes_Ano'] = pd.to_datetime(st.session_state.df_paxs_in['Data_Execucao']).dt.to_period('M')

    st.session_state.df_paxs_in['Total_Paxs'] = st.session_state.df_paxs_in['Total_ADT'].fillna(0) + (st.session_state.df_paxs_in['Total_CHD'].fillna(0) / 2)

    if st.session_state.base_luck == 'test_phoenix_joao_pessoa':  

        st.session_state.df_paxs_in = pd.merge(st.session_state.df_paxs_in, st.session_state.df_metas[['Mes_Ano', 'Paxs_Desc']], on='Mes_Ano', how='left')

def gerar_lista_setor():

    lista_setor = sorted(st.session_state.df_vendas_final['Setor'].str.strip().dropna().unique().tolist())

    lista_setor.insert(0, '--- Todos ---')

    return lista_setor

def colher_periodo_e_setor(col1):

    with col1:

        with st.container():

            col1_1, col1_2 = st.columns(2)

            with col1_1:

                primeiro_dia_mes = pd.to_datetime('today').to_period('M').start_time.date()

                data_ini = st.date_input('Data Início', value=primeiro_dia_mes, format='DD/MM/YYYY', key='data_ini_on')

                mes_ano_ini = pd.to_datetime(data_ini).to_period('M')

            with col1_2:

                ultimo_dia_mes = pd.to_datetime('today').to_period('M').end_time.date()
                
                data_fim = st.date_input('Data Fim', value=ultimo_dia_mes, format='DD/MM/YYYY', key='data_fim_on')  

                mes_ano_fim = pd.to_datetime(data_fim).to_period('M')

        seleciona_setor = st.multiselect('Setor', sorted(lista_setor), default=None, key='seleciona_setor')

        if st.session_state.base_luck=='test_phoenix_noronha':

            base_tm = st.multiselect('Base Ticket Médio', ['Paxs Recebidos', 'Número de Serviços'], default='Paxs Recebidos')

        else:

            base_tm = []

    return data_ini, data_fim, mes_ano_ini, mes_ano_fim, seleciona_setor, base_tm

def filtrar_periodo_dfs_base_luck(data_ini, data_fim, mes_ano_ini, mes_ano_fim):

    def filtrar_periodo_dfs(data_ini, data_fim, mes_ano_ini, mes_ano_fim):

        df_vendas = st.session_state.df_vendas_final[(st.session_state.df_vendas_final['Data_Venda'] >= data_ini) & (st.session_state.df_vendas_final['Data_Venda'] <= data_fim)].reset_index(drop=True)

        df_paxs_in = st.session_state.df_paxs_in[(st.session_state.df_paxs_in['Data_Execucao'] >= data_ini) & (st.session_state.df_paxs_in['Data_Execucao'] <= data_fim)].reset_index(drop=True)

        df_guias_in = st.session_state.df_guias_in[(st.session_state.df_guias_in['Data da Escala'] >= data_ini) & (st.session_state.df_guias_in['Data da Escala'] <= data_fim)]\
            .reset_index(drop=True)

        df_metas_vendedor = st.session_state.df_metas_vendedor[(st.session_state.df_metas_vendedor['Mes_Ano'] >= mes_ano_ini) & 
                                                            (st.session_state.df_metas_vendedor['Mes_Ano'] <= mes_ano_fim)].reset_index(drop=True)
            
        df_metas_setor = st.session_state.df_metas[(st.session_state.df_metas['Mes_Ano'] >= mes_ano_ini) & (st.session_state.df_metas['Mes_Ano'] <= mes_ano_fim)].reset_index(drop=True)

        if st.session_state.base_luck=='test_phoenix_natal':

            df_ocupacao_hoteis = st.session_state.df_ocupacao_hoteis[(st.session_state.df_ocupacao_hoteis['Mes_Ano'] >= mes_ano_ini) & 
                                                                    (st.session_state.df_ocupacao_hoteis['Mes_Ano'] <= mes_ano_fim)].reset_index(drop=True)
            
            return df_vendas, df_paxs_in, df_guias_in, df_metas_vendedor, df_metas_setor, df_ocupacao_hoteis

        elif st.session_state.base_luck=='test_phoenix_salvador':

            df_metas_vendedor_setor = st.session_state.df_metas_vendedor_setor[(st.session_state.df_metas_vendedor_setor['Mes_Ano'] >= mes_ano_ini) &
                                                                            (st.session_state.df_metas_vendedor_setor['Mes_Ano'] <= mes_ano_fim)].reset_index(drop=True)
            
            df_metas_vendedor_canal_vendas = st.session_state.df_metas_vendedor_canal_vendas[(st.session_state.df_metas_vendedor_canal_vendas['Mes_Ano'] >= mes_ano_ini) &
                                                                                            (st.session_state.df_metas_vendedor_canal_vendas['Mes_Ano'] <= mes_ano_fim)].reset_index(drop=True)

            return df_vendas, df_paxs_in, df_guias_in, df_metas_vendedor, df_metas_setor, df_metas_vendedor_setor, df_metas_vendedor_canal_vendas
        
        else:

            return df_vendas, df_paxs_in, df_guias_in, df_metas_vendedor, df_metas_setor

    def juntar_servicos_com_nomenclaturas_diferentes(df_vendas):

        dict_alterar_nomes_servicos = dict(zip(st.session_state.df_juntar_servicos['Serviço'], st.session_state.df_juntar_servicos['Serviço Principal']))
        
        df_vendas['Servico'] = df_vendas['Servico'].replace(dict_alterar_nomes_servicos)

        return df_vendas

    if st.session_state.base_luck=='test_phoenix_natal':

        df_vendas, df_paxs_in, df_guias_in, df_metas_vendedor, df_metas_setor, df_ocupacao_hoteis = filtrar_periodo_dfs(data_ini, data_fim, mes_ano_ini, mes_ano_fim)

        df_vendas = juntar_servicos_com_nomenclaturas_diferentes(df_vendas)

        return df_vendas, df_paxs_in, df_guias_in, df_metas_vendedor, df_metas_setor, df_ocupacao_hoteis

    elif st.session_state.base_luck in ['test_phoenix_joao_pessoa', 'test_phoenix_noronha']:

        df_vendas, df_paxs_in, df_guias_in, df_metas_vendedor, df_metas_setor = filtrar_periodo_dfs(data_ini, data_fim, mes_ano_ini, mes_ano_fim)

        return df_vendas, df_paxs_in, df_guias_in, df_metas_vendedor, df_metas_setor

    elif st.session_state.base_luck == 'test_phoenix_salvador':

        df_vendas, df_paxs_in, df_guias_in, df_metas_vendedor, df_metas_setor, df_metas_vendedor_setor, df_metas_vendedor_canal_vendas = \
            filtrar_periodo_dfs(data_ini, data_fim, mes_ano_ini, mes_ano_fim)

        df_vendas = juntar_servicos_com_nomenclaturas_diferentes(df_vendas)

        return df_vendas, df_paxs_in, df_guias_in, df_metas_vendedor, df_metas_setor, df_metas_vendedor_setor, df_metas_vendedor_canal_vendas

def filtrar_setores_selecionados(seleciona_setor, df_vendas):

    # Se selecionar um setor específico e tiver Guia selecionado, mas Transferista não tiver, aí vai adicionar Transferista automaticamente para as bases em que eles compoem as metas dos guias

    if not '--- Todos ---' in seleciona_setor \
        and 'Guia' in seleciona_setor \
        and not 'Transferista' in seleciona_setor \
        and st.session_state.base_luck in ['test_phoenix_joao_pessoa', 'test_phoenix_salvador']:

        lista_setor = seleciona_setor.copy()

        lista_setor.append('Transferista')

        df_vendas = df_vendas[df_vendas['Setor'].isin(lista_setor)]

    # Se as metas forem separadas, não precisa fazer nada e só filtra os setores selecionados

    elif not '--- Todos ---' in seleciona_setor:

        df_vendas = df_vendas[df_vendas['Setor'].isin(seleciona_setor)]

    return df_vendas

def colher_selecoes_vendedor_canal_hotel(df_vendas, col1):

    def criar_listas_vendedor_canal_hotel(df_vendas):

        lista_vendedor = sorted(df_vendas['Vendedor'].dropna().unique().tolist())

        lista_vendedor.insert(0, '--- Todos ---')

        lista_canal = sorted(df_vendas['Canal_de_Vendas'].dropna().unique().tolist())

        lista_canal.insert(0, '--- Todos ---')

        lista_hotel = sorted(df_vendas['Estabelecimento_Origem'].dropna().unique().tolist())

        lista_hotel.insert(0, '--- Todos ---')

        return lista_vendedor, lista_canal, lista_hotel

    lista_vendedor, lista_canal, lista_hotel = criar_listas_vendedor_canal_hotel(df_vendas)

    seleciona_canal = col1.multiselect('Canal de Vendas', lista_canal, key='Can_on')

    seleciona_vend = col1.multiselect('Vendedor', lista_vendedor, key='Ven_on')

    seleciona_hotel = col1.multiselect('Hotel', lista_hotel, key='Hot_on')

    if st.session_state.base_luck=='test_phoenix_natal':

        filtrar_servicos_terceiros = col1.multiselect('Filtrar Serviços de Terceiros', ['Sim'], default=None)

    else:

        filtrar_servicos_terceiros = []

    return seleciona_canal, seleciona_vend, seleciona_hotel, filtrar_servicos_terceiros

def filtrar_canal_vendedor_hotel_df_vendas(df_vendas, seleciona_canal, seleciona_vend, seleciona_hotel, filtrar_servicos_terceiros):
    
    if len(seleciona_canal)>0 and '--- Todos ---' not in seleciona_canal:

        df_vendas = df_vendas[df_vendas['Canal_de_Vendas'].isin(seleciona_canal)]

    if len(seleciona_vend)>0 and '--- Todos ---' not in seleciona_vend:

        df_vendas = df_vendas[df_vendas['Vendedor'].isin(seleciona_vend)]
    
    if len(seleciona_hotel)>0 and '--- Todos ---' not in seleciona_hotel:

        df_vendas = df_vendas[df_vendas['Estabelecimento_Origem'].isin(seleciona_hotel)]

    if len(filtrar_servicos_terceiros)>0:

        df_vendas = df_vendas[~df_vendas['Servico'].isin(st.session_state.servicos_terceiros)]

    return df_vendas

def gerar_df_hotel(df_vendas):

    df_hotel = df_vendas.copy()

    df_hotel['Estabelecimento_Origem'] = df_hotel['Estabelecimento_Origem'].fillna('')

    df_hotel['Estabelecimento_Destino'] = df_hotel['Estabelecimento_Destino'].fillna('')

    df_hotel.rename(columns={'Desconto_Global_Por_Servico': 'Desconto Reserva x Serviços', 'Valor_Venda': 'Vendas'}, inplace=True)

    df_hotel['Hotel'] = np.where(~df_hotel['Estabelecimento_Origem'].str.upper().str.contains('AEROPORTO'), df_hotel['Estabelecimento_Origem'], df_hotel['Estabelecimento_Destino'])

    df_hotel = df_hotel.groupby(['Mes_Ano','Vendedor', 'Hotel']).agg({'Vendas': 'sum', 'Desconto Reserva x Serviços': 'sum'}).reset_index()

    return df_hotel

def ajustar_desconto_global(df_vendas):

    if st.session_state.base_luck == 'test_phoenix_joao_pessoa':

        valor_ref = np.where(df_vendas['Data_Venda'] >= datetime.date(2024, 12, 1), 1000, 5000)

        df_vendas['Desconto_Global_Ajustado'] = np.where((df_vendas['Desconto_Global_Por_Servico'].notna()) & (df_vendas['Desconto_Global_Por_Servico'] < valor_ref) & 
                                                        (df_vendas['Servico'] != 'EXTRA'), df_vendas['Desconto_Global_Por_Servico'], 0)
        
    else:

        df_vendas['Desconto_Global_Ajustado'] = df_vendas['Desconto_Global_Por_Servico']
    
    return df_vendas

def calculando_soma_total_paxs_paxs_desc(df_paxs_in, df_metas_setor, df_vendas_agrupado):

    total_paxs_in = df_paxs_in['Total_Paxs'].sum()

    if st.session_state.base_luck in ['test_phoenix_joao_pessoa', 'test_phoenix_salvador']:

        total_paxs_desc = df_metas_setor['Paxs_Desc'].fillna(0).sum()

    else:

        total_paxs_desc = 0

    df_vendas_agrupado['Total_Paxs'] = total_paxs_in + total_paxs_desc

    return df_vendas_agrupado

def gerar_df_vendas_agrupado(df_vendas, df_metas_vendedor, df_guias_in, df_paxs_in, df_metas_setor, df_ocupacao_hoteis=None, df_metas_vendedor_setor=None):

    def inserindo_paxs_in_vendedor(df_vendas_agrupado, df_guias_in):

        df_guias_in = df_guias_in.rename(columns={'Total_Paxs': 'Paxs_IN', 'Guia': 'Vendedor'})

        df_vendas_agrupado = pd.merge(df_vendas_agrupado, df_guias_in[['Vendedor', 'Paxs_IN']], on='Vendedor', how='left')

        return df_vendas_agrupado

    def inserindo_meta_vendedor_periodo(df_metas_vendedor, df_vendas_agrupado):

        df_metas_vendedor_periodo = df_metas_vendedor.groupby('Vendedor', as_index=False)['Meta_Mes'].mean()

        df_vendas_agrupado = pd.merge(df_vendas_agrupado, df_metas_vendedor_periodo, on='Vendedor', how='left')

        if st.session_state.base_luck == 'test_phoenix_salvador':

            df_metas_vendedor_setor_periodo = df_metas_vendedor_setor.groupby(['Vendedor', 'Setor'], as_index=False)['Meta_Mes'].mean()

            df_metas_vendedor_setor_periodo.rename(columns={'Meta_Mes': 'Meta_Vendedor_Setor'}, inplace=True)

            df_vendas_agrupado = pd.merge(df_vendas_agrupado, df_metas_vendedor_setor_periodo, on=['Vendedor', 'Setor'], how='left')

            df_vendas_agrupado['Meta_Vendedor_Setor'] = df_vendas_agrupado['Meta_Vendedor_Setor'].fillna(df_vendas_agrupado['Meta_Mes'])

        return df_vendas_agrupado

    df_vendas_agrupado = df_vendas.groupby(['Vendedor', 'Setor'], dropna=False).agg({'Valor_Venda': 'sum', 'Valor_Reembolso': 'sum', 'Desconto_Global_Ajustado': 'sum', 'Meta': 'mean', 
                                                                                     'Servico': 'count', 'Reserva': 'nunique'}).reset_index()
    
    df_vendas_agrupado = inserindo_paxs_in_vendedor(df_vendas_agrupado, df_guias_in)

    df_vendas_agrupado = calculando_soma_total_paxs_paxs_desc(df_paxs_in, df_metas_setor, df_vendas_agrupado)

    df_vendas_agrupado['Venda_Filtrada'] = df_vendas_agrupado['Valor_Venda'].fillna(0) - df_vendas_agrupado['Valor_Reembolso'].fillna(0)

    df_vendas_agrupado['Venda_por_Reserva'] = df_vendas_agrupado['Servico'] / df_vendas_agrupado['Reserva']

    if st.session_state.base_luck in ['test_phoenix_joao_pessoa', 'test_phoenix_salvador']:

        df_vendas_agrupado['Ticket_Medio'] = np.where(df_vendas_agrupado['Setor'].isin(['Guia', 'Transferista']), df_vendas_agrupado['Venda_Filtrada'] / df_vendas_agrupado['Paxs_IN'], 
                                                      df_vendas_agrupado['Venda_Filtrada'] / df_vendas_agrupado['Total_Paxs'])
        
        df_vendas_agrupado['Ticket_Medio'] = df_vendas_agrupado['Ticket_Medio'].fillna(0)

    elif st.session_state.base_luck == 'test_phoenix_noronha':

        if len(base_tm)==0 or base_tm[0]=='Paxs Recebidos':

            df_vendas_agrupado['Ticket_Medio'] = df_vendas_agrupado['Venda_Filtrada'] / df_vendas_agrupado['Total_Paxs']

        elif base_tm[0]=='Número de Serviços':

            df_vendas_agrupado['Ticket_Medio'] = df_vendas_agrupado['Venda_Filtrada'] / df_vendas_agrupado['Servico']
        
        df_vendas_agrupado['Ticket_Medio'] = df_vendas_agrupado['Ticket_Medio'].fillna(0)

    elif st.session_state.base_luck == 'test_phoenix_natal':

        df_ocupacao_hoteis = df_ocupacao_hoteis.groupby('Vendedor', as_index=False)['Paxs Hotel'].sum()

        df_vendas_agrupado = df_vendas_agrupado.merge(df_ocupacao_hoteis, on='Vendedor', how='left')

        df_vendas_agrupado['Ticket_Medio'] = np.where(
            df_vendas_agrupado['Setor'].isin(['Guia', 'Transferista']), 
            df_vendas_agrupado['Venda_Filtrada'] / df_vendas_agrupado['Paxs_IN'],
            np.where(
                pd.notna(df_vendas_agrupado['Paxs Hotel']),
                df_vendas_agrupado['Venda_Filtrada'] / df_vendas_agrupado['Paxs Hotel'],
                df_vendas_agrupado['Venda_Filtrada'] / df_vendas_agrupado['Total_Paxs']
                )
            )
        
        df_vendas_agrupado['Ticket_Medio'] = df_vendas_agrupado['Ticket_Medio'].fillna(0)
    
    df_vendas_agrupado = df_vendas_agrupado.sort_values(by='Venda_Filtrada', ascending=False)

    df_vendas_agrupado = inserindo_meta_vendedor_periodo(df_metas_vendedor, df_vendas_agrupado)

    return df_vendas_agrupado

def formatar_moeda(valor):

    return format_currency(valor, 'BRL', locale='pt_BR')

def gerar_soma_vendas_tm_vendas_desconto_paxs_recebidos(df_vendas_agrupado):

    def gerar_media_descontos(total_desconto, soma_vendas):

        total_desconto, soma_vendas = float(total_desconto), float(soma_vendas)

        if total_desconto == 0 and soma_vendas == 0:

            return '0%'
        
        med_desconto = (total_desconto / (soma_vendas + total_desconto)) * 100

        return f'{round(med_desconto, 2)}%'

    def escolher_paxs_ref(row):

        if row['Setor']=='Transferista' or \
            (st.session_state.base_luck == 'test_phoenix_natal' and row['Setor']=='Guia'):

            return row['Paxs_IN']
        
        elif st.session_state.base_luck == 'test_phoenix_natal' and row['Setor']=='Desks':

            return row['Paxs Hotel']
        
        else:

            return row['Total_Paxs']
        
    def soma_ou_media_paxs(group):

        if group['Setor'].iloc[0] == 'Transferista' or \
            (st.session_state.base_luck == 'test_phoenix_natal' and group['Setor'].iloc[0] in ['Desks', 'Guia']):

            paxs_ref_tm = group['Paxs_Ref_TM'].sum()

        else:

            paxs_ref_tm = group['Paxs_Ref_TM'].mean()

        meta = group['Meta'].mean()
        
        return pd.Series({
            'Paxs_Ref_TM': paxs_ref_tm,
            'Meta': meta
        })

    df_vendas_setores_desejados = df_vendas_agrupado[pd.notna(df_vendas_agrupado['Setor'])]
        
    soma_vendas = df_vendas_setores_desejados['Venda_Filtrada'].sum()

    df_vendas_setores_desejados['Paxs_Ref_TM'] = df_vendas_setores_desejados.apply(escolher_paxs_ref, axis=1)

    df_vendas_setores_desejados['Paxs_Ref_TM'] = df_vendas_setores_desejados['Paxs_Ref_TM'].fillna(0)

    if len(seleciona_setor)==1 and seleciona_setor[0]!='--- Todos ---':

        if (seleciona_setor[0]=='Transferista' 
            and st.session_state.base_luck != 'test_phoenix_noronha') \
            or (st.session_state.base_luck == 'test_phoenix_natal' 
                and seleciona_setor[0] in ['Desks', 'Guia']):

            tm_vendas = soma_vendas / df_vendas_setores_desejados['Paxs_Ref_TM'].fillna(0).sum()

            paxs_recebidos = int(df_vendas_setores_desejados['Paxs_Ref_TM'].fillna(0).sum())

        else:

            if st.session_state.base_luck == 'test_phoenix_noronha' and len(base_tm)>0 and base_tm[0]=='Número de Serviços':

                tm_vendas = soma_vendas / df_vendas_setores_desejados['Servico'].sum()

            else:

                tm_vendas = soma_vendas / df_vendas_setores_desejados['Total_Paxs'].mean()

            paxs_recebidos = int(df_vendas_setores_desejados['Total_Paxs'].mean())

        df_setor_meta = df_vendas_setores_desejados.groupby('Setor', as_index=False)['Meta'].first()

        if st.session_state.base_luck == 'test_phoenix_joao_pessoa' and seleciona_setor[0]=='Guia':

            tm_setor_estip = df_setor_meta[df_setor_meta['Setor']!='Transferista']['Meta'].sum()

        elif st.session_state.base_luck == 'test_phoenix_natal' and seleciona_setor[0]=='Vendas Online':

            tm_setor_estip = df_setor_meta['Meta'].sum() / paxs_recebidos

        elif st.session_state.base_luck == 'test_phoenix_noronha':

            if len(base_tm)>0 and base_tm[0]=='Número de Serviços':

                tm_setor_estip = df_setor_meta['Meta'].sum() / df_vendas_setores_desejados['Servico'].sum()

            else:

                tm_setor_estip = df_setor_meta['Meta'].sum() / paxs_recebidos

        else:

            tm_setor_estip = df_setor_meta['Meta'].sum()

        if st.session_state.base_luck == 'test_phoenix_joao_pessoa':

            total_desconto = df_vendas_setores_desejados[df_vendas_setores_desejados['Servico'] != 'EXTRA']['Desconto_Global_Ajustado'].sum()

        else:

            total_desconto = df_vendas_setores_desejados['Desconto_Global_Ajustado'].sum()

        if st.session_state.base_luck == 'test_phoenix_natal':

            df_vendas_agrupado.loc[df_vendas_agrupado['Setor']=='Vendas Online', 'Meta'] = df_vendas_agrupado.loc[df_vendas_agrupado['Setor']=='Vendas Online', 'Meta'] / \
                df_vendas_agrupado.loc[df_vendas_agrupado['Setor']=='Vendas Online', 'Total_Paxs']

        med_desconto = gerar_media_descontos(total_desconto, soma_vendas)

        if st.session_state.base_luck == 'test_phoenix_noronha' and len(base_tm)>0 and base_tm[0]=='Número de Serviços':

            meta_esperada_total = tm_setor_estip*df_vendas_setores_desejados['Servico'].sum()

        else:

            meta_esperada_total = tm_setor_estip*paxs_recebidos

        meta_esperada_formatada = formatar_moeda(meta_esperada_total)

        perc_alcancado = f'{round((soma_vendas / meta_esperada_total) * 100, 2)}%'

    elif len(seleciona_setor)>1 or (len(seleciona_setor)==1 and seleciona_setor[0]=='--- Todos ---'):

        df_vendas_esperadas = df_vendas_setores_desejados.groupby('Setor', as_index=False).apply(soma_ou_media_paxs)

        if st.session_state.base_luck == 'test_phoenix_natal':

            df_vendas_esperadas.loc[df_vendas_esperadas['Setor']=='Vendas Online', 'Meta'] = df_vendas_esperadas.loc[df_vendas_esperadas['Setor']=='Vendas Online', 'Meta'] / \
                df_vendas_esperadas.loc[df_vendas_esperadas['Setor']=='Vendas Online', 'Paxs_Ref_TM']

            df_vendas_agrupado.loc[df_vendas_agrupado['Setor']=='Vendas Online', 'Meta'] = df_vendas_agrupado.loc[df_vendas_agrupado['Setor']=='Vendas Online', 'Meta'] / \
                df_vendas_agrupado.loc[df_vendas_agrupado['Setor']=='Vendas Online', 'Total_Paxs']

        if 'Guia' in df_vendas_esperadas['Setor'].tolist() and st.session_state.base_luck == 'test_phoenix_joao_pessoa':

            df_vendas_esperadas = df_vendas_esperadas[df_vendas_esperadas['Setor']!='Transferista']

        if st.session_state.base_luck != 'test_phoenix_noronha':

            df_vendas_esperadas['Venda Esperada Individual'] = df_vendas_esperadas['Meta'] * df_vendas_esperadas['Paxs_Ref_TM']

        else:

            df_vendas_esperadas['Venda Esperada Individual'] = df_vendas_esperadas['Meta']

        meta_esperada_total = df_vendas_esperadas['Venda Esperada Individual'].sum()

        meta_esperada_formatada = formatar_moeda(meta_esperada_total)

        perc_alcancado = f'{round((soma_vendas / meta_esperada_total) * 100, 2)}%'

        if (
            st.session_state.base_luck == 'test_phoenix_natal' 
            and 'Guia' in seleciona_setor 
            and 'Transferista' in seleciona_setor
            and len(seleciona_setor)==2
        ):
            
            paxs_recebidos = int(df_vendas_setores_desejados['Paxs_Ref_TM'].sum())

            tm_vendas = soma_vendas / paxs_recebidos

            tm_setor_estip = int(meta_esperada_total / paxs_recebidos)
            
        elif (
            st.session_state.base_luck == 'test_phoenix_noronha' 
            and len(base_tm)>0 and base_tm[0]=='Número de Serviços'
        ):

            tm_vendas = soma_vendas / df_vendas_setores_desejados['Servico'].sum()

            tm_setor_estip = int(meta_esperada_total / df_vendas_setores_desejados['Servico'].sum())

        else:

            paxs_recebidos = int(df_vendas_setores_desejados['Total_Paxs'].mean())

            tm_vendas = soma_vendas / df_vendas_setores_desejados['Total_Paxs'].mean()

            tm_setor_estip = int(meta_esperada_total / paxs_recebidos)

    if st.session_state.base_luck == 'test_phoenix_joao_pessoa':

        total_desconto = df_vendas_setores_desejados[df_vendas_setores_desejados['Servico'] != 'EXTRA']['Desconto_Global_Ajustado'].sum()

    else:

        total_desconto = df_vendas_setores_desejados['Desconto_Global_Ajustado'].sum()

    med_desconto = gerar_media_descontos(total_desconto, soma_vendas)

    return soma_vendas, tm_vendas, tm_setor_estip, total_desconto, paxs_recebidos, med_desconto, meta_esperada_formatada, perc_alcancado

def plotar_todos_quadrados_html(col2, soma_vendas, meta_esperada_formatada, tm_setor_estip, total_desconto, perc_alcancado, paxs_recebidos, tm_vendas, med_desconto):

    def plotar_quadrados_html(titulo, info_numero):
        
        st.markdown(f"""
        <div style="background-color:#f0f0f5; border-radius:10px; border: 2px solid #ccc; text-align: center; width: 180px; margin:0 auto; margin: 0 auto 10px auto; min-height: 50px;">
            <h3 style="color: #333; font-size: 18px; padding: 0px 10px; text-align: center; margin-bottom: 0px; ">{titulo}</h3>
            <h2 style="color: #047c6c; font-size: 20px; padding: 10px 30px; text-align: center; margin:0 auto; white-space: nowrap;">{info_numero}</h2>
        </div>
        """, unsafe_allow_html=True)

    with col2:
            
        col2_1, col2_2 = st.columns([2,5])

        with col2_1:

            with st.container():

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

def gerar_df_estilizado(df_vendas_agrupado):
    
    def highlight_ticket(row):

        if row['Ticket Médio'] > row['Meta T.M.'] and row['Meta T.M.']>0:

            return ['background-color: lightgreen'] * len(row)
        
        else:

            return [''] * len(row)

    if st.session_state.base_luck == 'test_phoenix_salvador' \
        and len(seleciona_setor)==1 \
            and seleciona_setor[0]=='--- Todos ---':
        
        df_estilizado = df_vendas_agrupado.groupby('Vendedor', as_index=False).agg({'Venda_Filtrada': 'sum', 'Ticket_Medio': 'sum', 'Meta_Mes': 'first', 'Meta': 'first', 'Servico': 'sum', 
                                                                                    'Reserva': 'sum', 'Desconto_Global_Ajustado': 'first'})

        df_estilizado['Venda_por_Reserva'] = df_estilizado['Servico'] / df_estilizado['Reserva']

        df_estilizado['Meta_Mes'] = df_estilizado['Meta_Mes'].replace(0, None).fillna(df_estilizado['Meta'])
        
        df_estilizado = df_estilizado[['Vendedor', 'Venda_Filtrada', 'Ticket_Medio', 'Meta_Mes', 'Venda_por_Reserva', 'Desconto_Global_Ajustado']]

        df_estilizado.columns = [
            'Vendedor', 
            'Vendas', 
            'Ticket Médio', 
            'Meta T.M.', 
            'Venda por Reserva', 
            'R$ Descontos',
        ]

    elif st.session_state.base_luck == 'test_phoenix_salvador' \
        and len(seleciona_setor)==1 \
            and seleciona_setor[0]!='--- Todos ---':

        df_estilizado = df_vendas_agrupado[['Vendedor', 'Venda_Filtrada', 'Ticket_Medio', 'Meta_Mes', 'Meta_Vendedor_Setor', 'Meta', 'Venda_por_Reserva', 'Desconto_Global_Ajustado']].copy()

        df_estilizado['Meta_Mes'] = np.where(
            pd.notna(df_estilizado['Meta_Vendedor_Setor']), 
            df_estilizado['Meta_Vendedor_Setor'], 
            np.where(
                pd.isna(df_estilizado['Meta_Mes']), 
                df_estilizado['Meta'], 
                df_estilizado['Meta_Mes']
                )
            )

        df_estilizado = df_estilizado.drop(['Meta_Vendedor_Setor', 'Meta'], axis=1)

        df_estilizado.columns = [
            'Vendedor', 
            'Vendas', 
            'Ticket Médio', 
            'Meta T.M.', 
            'Venda por Reserva', 
            'R$ Descontos',
        ]

    elif st.session_state.base_luck == 'test_phoenix_salvador' \
        and len(seleciona_setor)>1:

        df_estilizado = df_vendas_agrupado[['Vendedor', 'Venda_Filtrada', 'Ticket_Medio', 'Meta_Mes', 'Meta_Vendedor_Setor', 'Meta', 'Venda_por_Reserva', 'Desconto_Global_Ajustado', 'Reserva', 
                                            'Servico']].copy()

        df_estilizado['Meta_Mes'] = np.where(
            pd.notna(df_estilizado['Meta_Vendedor_Setor']), 
            df_estilizado['Meta_Vendedor_Setor'], 
            np.where(
                pd.isna(df_estilizado['Meta_Mes']), 
                df_estilizado['Meta'], 
                df_estilizado['Meta_Mes']
                )
            )

        df_estilizado = df_estilizado.drop(['Meta_Vendedor_Setor', 'Meta'], axis=1)

        df_estilizado = df_estilizado.groupby('Vendedor', as_index=False).agg({'Venda_Filtrada': 'sum', 'Ticket_Medio': 'sum', 'Meta_Mes': 'sum', 'Servico': 'sum', 
                                                                               'Reserva': 'sum', 'Desconto_Global_Ajustado': 'sum'})
        
        df_estilizado['Venda_por_Reserva'] = df_estilizado['Servico'] / df_estilizado['Reserva']

        df_estilizado = df_estilizado[['Vendedor', 'Venda_Filtrada', 'Ticket_Medio', 'Meta_Mes', 'Venda_por_Reserva', 'Desconto_Global_Ajustado']]

        df_estilizado.columns = [
            'Vendedor', 
            'Vendas', 
            'Ticket Médio', 
            'Meta T.M.', 
            'Venda por Reserva', 
            'R$ Descontos',
        ]

    elif st.session_state.base_luck == 'test_phoenix_noronha':

        df_estilizado = df_vendas_agrupado[['Vendedor', 'Venda_Filtrada', 'Ticket_Medio', 'Meta_Mes', 'Venda_por_Reserva', 'Desconto_Global_Ajustado', 'Servico']].copy()

        df_estilizado.columns = [
            'Vendedor', 
            'Vendas', 
            'Ticket Médio', 
            'Meta T.M.', 
            'Venda por Reserva', 
            'R$ Descontos',
            'Servico'
        ]

    else:

        df_estilizado = df_vendas_agrupado[['Vendedor', 'Venda_Filtrada', 'Ticket_Medio', 'Meta_Mes', 'Venda_por_Reserva', 'Desconto_Global_Ajustado']].copy()

        df_estilizado.columns = [
            'Vendedor', 
            'Vendas', 
            'Ticket Médio', 
            'Meta T.M.', 
            'Venda por Reserva', 
            'R$ Descontos',
        ]

    df_estilizado['Meta T.M.'] = df_estilizado['Meta T.M.'].replace(0, None).fillna(df_vendas_agrupado['Meta'])

    df_estilizado['% Descontos'] = (df_estilizado['R$ Descontos'] / (df_estilizado['Vendas'] + df_estilizado['R$ Descontos']))*100

    df_estilizado['% Descontos'] = df_estilizado['% Descontos'].fillna(0)

    df_estilizado = df_estilizado.drop_duplicates(keep='last')

    if st.session_state.base_luck == 'test_phoenix_noronha':

        if len(base_tm)==0 or base_tm[0]=='Paxs Recebidos':

            df_estilizado['Meta T.M.'] = df_estilizado['Meta T.M.'] / paxs_recebidos

        elif base_tm[0]=='Número de Serviços':

            df_estilizado['Meta T.M.'] = df_estilizado['Meta T.M.'] / df_estilizado['Servico']

        df_estilizado.drop('Servico', axis=1, inplace=True)

    df_estilizado = df_estilizado.style.apply(highlight_ticket, axis=1)

    df_estilizado = df_estilizado.format({'Vendas': formatar_moeda, 'Ticket Médio': formatar_moeda, 'Meta T.M.': formatar_moeda, 'Venda por Reserva': '{:.2f}'.format, 
                                          'R$ Descontos': formatar_moeda, '% Descontos':'{:.2f}%'.format})
    
    return df_estilizado

def plotar_vendas_por_vendedor(df_estilizado):

    st.subheader('Vendas por Vendedor')

    st.dataframe(
        df_estilizado, 
        hide_index=True, 
        use_container_width=True
    )

def plotar_vendas_por_vendedor_setor_canal_vendas(seleciona_setor, df_vendas_agrupado, row1, i, df_vendas, df_paxs_in, df_metas_vendedor=None, df_metas_setor=None, 
                                                    df_metas_vendedor_canal_vendas=None):
    
    def gerar_df_estilizado_vendedor_setor(df_vendas_agrupado):
        
        def highlight_ticket(row):

            if row['Ticket Médio'] > row['Meta T.M.'] and row['Meta T.M.']>0:

                return ['background-color: lightgreen'] * len(row)
            
            else:

                return [''] * len(row)

        df_estilizado = df_vendas_agrupado.groupby(['Vendedor', 'Setor'], as_index=False).agg({'Venda_Filtrada': 'sum', 'Total_Paxs': 'mean', 'Meta_Vendedor_Setor': 'sum', 'Servico': 'sum', 
                                                                                            'Reserva': 'sum', 'Desconto_Global_Ajustado': 'first'})
        
        df_estilizado['Ticket_Medio'] = df_estilizado['Venda_Filtrada'] / df_estilizado['Total_Paxs']

        df_estilizado['Venda_por_Reserva'] = df_estilizado['Servico'] / df_estilizado['Reserva']

        df_estilizado = df_estilizado[['Vendedor', 'Setor', 'Venda_Filtrada', 'Ticket_Medio', 'Meta_Vendedor_Setor', 'Venda_por_Reserva', 'Desconto_Global_Ajustado']]

        df_estilizado['% Desconto'] = (df_estilizado['Desconto_Global_Ajustado'] / (df_estilizado['Venda_Filtrada'] + df_estilizado['Desconto_Global_Ajustado']))*100

        df_estilizado['% Desconto'] = df_estilizado['% Desconto'].fillna(0)

        df_estilizado = df_estilizado.drop_duplicates(keep='last')

        df_estilizado.columns = ['Vendedor', 'Setor', 'Vendas', 'Ticket Médio', 'Meta T.M.', 'Venda por Reserva', 'R$ Descontos', '% Descontos']

        df_estilizado = df_estilizado.style.apply(highlight_ticket, axis=1)

        df_estilizado = df_estilizado.format({'Vendas': formatar_moeda, 'Ticket Médio': formatar_moeda, 'Meta T.M.': formatar_moeda, 'Venda por Reserva': '{:.2f}'.format, 
                                            'R$ Descontos': formatar_moeda, '% Descontos':'{:.2f}%'.format})
        
        return df_estilizado

    def gerar_df_estilizado_vendedor_canal_vendas(df_metas_vendedor_canal_vendas, df_vendas, df_metas_vendedor, df_metas_setor, df_paxs_in):
        
        def highlight_ticket(row):

            if row['Ticket Médio'] > row['Meta T.M.'] and row['Meta T.M.']>0:

                return ['background-color: lightgreen'] * len(row)
            
            else:

                return [''] * len(row)
            
        df_estilizado = df_vendas.groupby(['Vendedor', 'Canal_de_Vendas'], dropna=False).agg({'Valor_Venda': 'sum', 'Valor_Reembolso': 'sum', 'Desconto_Global_Ajustado': 'sum', 'Meta': 'mean', 
                                                                                            'Servico': 'count', 'Reserva': 'nunique'}).reset_index()
        
        df_estilizado = calculando_soma_total_paxs_paxs_desc(df_paxs_in, df_metas_setor, df_estilizado)

        df_estilizado['Venda_Filtrada'] = df_estilizado['Valor_Venda'].fillna(0) - df_estilizado['Valor_Reembolso'].fillna(0)

        df_estilizado['Venda_por_Reserva'] = df_estilizado['Servico'] / df_estilizado['Reserva']

        if len(base_tm)==0 or base_tm[0]=='Paxs Recebidos':

            df_estilizado['Ticket_Medio'] = df_estilizado['Venda_Filtrada'] / df_estilizado['Total_Paxs']

        elif base_tm[0]=='Número de Serviços':

            df_estilizado['Ticket_Medio'] = df_estilizado['Venda_Filtrada'] / df_estilizado['Servico']

        df_metas_vendedor_periodo = df_metas_vendedor.groupby('Vendedor', as_index=False)['Meta_Mes'].mean()

        df_estilizado = pd.merge(df_estilizado, df_metas_vendedor_periodo, on='Vendedor', how='left')

        if st.session_state.base_luck == 'test_phoenix_salvador':
            
            df_metas_vendedor_canal_vendas_periodo = df_metas_vendedor_canal_vendas.groupby(['Vendedor', 'Canal_de_Vendas'], as_index=False)['Meta_Mes'].mean()

            df_metas_vendedor_canal_vendas_periodo.rename(columns={'Meta_Mes': 'Meta_Vendedor_Canal_Vendas'}, inplace=True)

            df_estilizado = pd.merge(df_estilizado, df_metas_vendedor_canal_vendas_periodo, on=['Vendedor', 'Canal_de_Vendas'], how='left')

            df_estilizado = df_estilizado[['Vendedor', 'Canal_de_Vendas', 'Venda_Filtrada', 'Ticket_Medio', 'Meta_Mes', 'Meta_Vendedor_Canal_Vendas', 'Meta', 'Venda_por_Reserva', 
                                        'Desconto_Global_Ajustado']]
            
            df_estilizado['Meta_Mes'] = np.where(
                pd.notna(df_estilizado['Meta_Vendedor_Canal_Vendas']), 
                df_estilizado['Meta_Vendedor_Canal_Vendas'], 
                np.where(
                    pd.isna(df_estilizado['Meta_Mes']), 
                    df_estilizado['Meta'], 
                    df_estilizado['Meta_Mes']
                    )
                )
            
            df_estilizado = df_estilizado.drop(['Meta_Vendedor_Canal_Vendas', 'Meta'], axis=1)

        elif st.session_state.base_luck == 'test_phoenix_noronha':

            df_estilizado = df_estilizado[['Vendedor', 'Canal_de_Vendas', 'Venda_Filtrada', 'Ticket_Medio', 'Meta_Mes', 'Meta', 'Venda_por_Reserva', 'Desconto_Global_Ajustado', 'Servico', 
                                           'Total_Paxs']]

            df_estilizado['Meta_Mes'] = np.where(
                pd.isna(df_estilizado['Meta_Mes']), 
                df_estilizado['Meta'], 
                df_estilizado['Meta_Mes']
            )

            if len(base_tm)==0 or base_tm[0]=='Paxs Recebidos':

                df_estilizado['Meta_Mes'] = df_estilizado['Meta_Mes'] / df_estilizado['Total_Paxs']

            elif base_tm[0]=='Número de Serviços':

                df_estilizado['Meta_Mes'] = df_estilizado['Meta_Mes'] / df_estilizado['Servico']

            df_estilizado = df_estilizado.drop(['Meta', 'Servico', 'Total_Paxs'], axis=1)

        else:

            df_estilizado = df_estilizado[['Vendedor', 'Canal_de_Vendas', 'Venda_Filtrada', 'Ticket_Medio', 'Meta_Mes', 'Meta', 'Venda_por_Reserva', 'Desconto_Global_Ajustado']]

            df_estilizado['Meta_Mes'] = np.where(
                pd.isna(df_estilizado['Meta_Mes']), 
                df_estilizado['Meta'], 
                df_estilizado['Meta_Mes']
            )

            df_estilizado = df_estilizado.drop(['Meta'], axis=1)

        df_estilizado['% Desconto'] = (df_estilizado['Desconto_Global_Ajustado'] / (df_estilizado['Venda_Filtrada'] + df_estilizado['Desconto_Global_Ajustado']))*100

        df_estilizado['% Desconto'] = df_estilizado['% Desconto'].fillna(0)

        df_estilizado = df_estilizado.drop_duplicates(keep='last')

        df_estilizado.columns = ['Vendedor', 'Canal de Vendas', 'Vendas', 'Ticket Médio', 'Meta T.M.', 'Venda por Reserva', 'R$ Descontos', '% Descontos']

        df_estilizado = df_estilizado.style.apply(highlight_ticket, axis=1)

        df_estilizado = df_estilizado.format({'Vendas': formatar_moeda, 'Ticket Médio': formatar_moeda, 'Meta T.M.': formatar_moeda, 'Venda por Reserva': '{:.2f}'.format, 
                                            'R$ Descontos': formatar_moeda, '% Descontos':'{:.2f}%'.format})
        
        return df_estilizado

    if st.session_state.base_luck == 'test_phoenix_salvador' \
        and (len(seleciona_setor)>1 or seleciona_setor[0] == '--- Todos ---'):

        df_estilizado_vendedor_setor = gerar_df_estilizado_vendedor_setor(df_vendas_agrupado)

        with row1[i%2]:

            st.subheader('Vendas por Vendedor | Setor')

            st.dataframe(
                df_estilizado_vendedor_setor, 
                hide_index=True, 
                use_container_width=True
            )

            i+=1

    elif st.session_state.base_luck in ['test_phoenix_salvador', 'test_phoenix_joao_pessoa', 'test_phoenix_noronha'] \
        and len(seleciona_setor)==1 and seleciona_setor[0] == 'Vendas Online':

        if df_metas_vendedor_canal_vendas is None:

            df_metas_vendedor_canal_vendas = pd.DataFrame(columns=df_vendas.columns)

        df_estilizado_vendedor_canal_vendas = gerar_df_estilizado_vendedor_canal_vendas(
            df_metas_vendedor_canal_vendas, 
            df_vendas, 
            df_metas_vendedor, 
            df_metas_setor, 
            df_paxs_in
        )

        with row1[i%2]:

            st.subheader('Vendas por Vendedor | Canal de Vendas')

            st.dataframe(
                df_estilizado_vendedor_canal_vendas, 
                hide_index=True, 
                use_container_width=True
            )

            i+=1

    return i

def plotar_vendas_por_hotel(row1, i, df_hotel):

    with row1[i%2]:

        st.subheader('Vendas por Hotel')

        df_hotel[['Vendas', 'Desconto Reserva x Serviços']] = df_hotel[['Vendas', 'Desconto Reserva x Serviços']].applymap(formatar_moeda)

        st.dataframe(
            df_hotel[['Vendedor', 'Hotel', 'Vendas']], 
            hide_index=True, 
            use_container_width=True
            )

        i+=1

    return i

def plotar_grafico_todos_setores(row1, i, df_setor_agrupado):

    with row1[i%2]:

        fig = gerar_grafico_todos_setores(df_setor_agrupado)

        st.plotly_chart(fig)

        i+=1

    return i

def plotar_grafico_pizza_todos_setores(row1, i, df_setor_agrupado):

    with row1[i%2]:

        fig_2 = gerar_grafico_pizza_todos_setores(df_setor_agrupado)

        st.plotly_chart(fig_2)

        i+=1

    return i

def gerar_df_vendas_vo_cv(df_vendas):

    df_vendas_vo_cv = df_vendas.copy()

    df_vendas_vo_cv['Venda_Filtrada'] = df_vendas_vo_cv['Valor_Venda'].fillna(0) - df_vendas_vo_cv['Valor_Reembolso'].fillna(0)

    df_vendas_vo_cv = df_vendas_vo_cv[df_vendas_vo_cv['Setor']=='Vendas Online'].groupby('Canal_de_Vendas')['Venda_Filtrada'].sum().reset_index()

    df_vendas_vo_cv.rename(columns={'Canal_de_Vendas': 'Setor'}, inplace=True)

    return df_vendas_vo_cv

def plotar_grafico_todos_setores_vo_destrinchado(df_setor_agrupado, df_vendas_vo_cv, row1, i):

    df_setor_vo_cv = df_setor_agrupado[df_setor_agrupado['Setor']!='Vendas Online'].reset_index(drop=True)

    df_setor_vo_cv = pd.concat(
        [df_setor_vo_cv, df_vendas_vo_cv], 
        ignore_index=True
    )

    df_setor_vo_cv = df_setor_vo_cv.sort_values(by='Setor')

    with row1[i%2]:

        fig = gerar_grafico_todos_setores(df_setor_vo_cv, 'Valor Total por Setor - Vendas Online Destrinchado')

        st.plotly_chart(fig)

        i+=1

    with row1[i%2]:

        fig_2 = gerar_grafico_pizza_todos_setores(df_setor_vo_cv, 'Participação % por Setor - Vendas Online Destrinchado')

        st.plotly_chart(fig_2)

        i+=1

    return i

def plotar_grafico_setor_guia(df_vendas_agrupado, row1, i):

    df_vendas_grafico = df_vendas_agrupado[(pd.notna(df_vendas_agrupado['Paxs_IN'])) & (df_vendas_agrupado['Paxs_IN']>=20)]

    if len(df_vendas_grafico)==0:

        df_vendas_grafico = df_vendas_agrupado

    with row1[i%2]:

        fig = gerar_grafico_setor_especifico(df_vendas_grafico)

        st.plotly_chart(fig)

        i+=1

    return i

def plotar_grafico_setor_vendas_online(df_vendas_agrupado, row1, i, df_vendas):

    with row1[i%2]:

        fig = gerar_grafico_setor_especifico(df_vendas_agrupado)

        st.plotly_chart(fig)

        i+=1

    with row1[i%2]:

        df_vendas_vo_cv = gerar_df_vendas_vo_cv(df_vendas)

        fig_2 = gerar_grafico_pizza_todos_setores(df_vendas_vo_cv)

        st.plotly_chart(fig_2)

        i+=1

    return i

def plotar_grafico_setor_especifico(df_vendas_agrupado, row1, i):

    with row1[i%2]:

        fig = gerar_grafico_setor_especifico(df_vendas_agrupado)

        st.plotly_chart(fig)

        i+=1

    return i

def gerar_df_servicos_casa_vs_terceiros(df_cont_passeio, colunas_group_by):

    df_servicos_casa_vs_terceiros = df_cont_passeio.copy()

    df_servicos_casa_vs_terceiros['Serviços Terceiros'] = df_servicos_casa_vs_terceiros['Servico'].apply(lambda x: 'Serviços Terceiros' 
                                                                                                            if x in st.session_state.servicos_terceiros 
                                                                                                            else 'Serviços da Casa')

    df_servicos_casa_vs_terceiros = df_servicos_casa_vs_terceiros.groupby(colunas_group_by, as_index=False)['Total Paxs'].sum()

    return df_servicos_casa_vs_terceiros

def plotar_todos_graficos_pizza_todos_setores(df_cont_passeio, row0):

    def gerar_df_todos_vendedores_filtrado(df_cont_passeio, passeios_incluidos):

        df_todos_vendedores_filtrado = df_cont_passeio[df_cont_passeio['Servico'].isin(passeios_incluidos)]

        if st.session_state.base_luck=='test_phoenix_joao_pessoa':

            df_todos_vendedores_filtrado['Servico'] = df_todos_vendedores_filtrado['Servico'].replace({'EMBARCAÇAO - CATAMARÃ DO FORRÓ ': 'CATAMARÃ DO FORRÓ', 
                                                                                                    'INGRESSO - BY NIGHT ': 'BY NIGHT PARAHYBA OXENTE '}) 

        return df_todos_vendedores_filtrado

    def gerar_grafico_pizza_servicos_terceiros_da_casa_todos_vendedores(df):

        fig = px.pie(
            df, 
            names='Serviços Terceiros', 
            values='Total Paxs', 
            title='Distribuição de Paxs - Terceiros vs Da Casa'
            )

        fig.update_traces(
            texttemplate='%{percent}', 
            hovertemplate='%{label}: %{value} Paxs'
            )

        fig.update_layout(
            showlegend=True, 
            margin=dict(t=50, b=50, l=50, r=50)
            )

        return fig

    def gerar_grafico_pizza_todos_vendedores(df_todos_vendedores_filtrado, passeios_incluidos):

        fig = px.pie(
            df_todos_vendedores_filtrado, 
            names='Servico', 
            values='Total Paxs', 
            title='Distribuição de Paxs por Passeio', 
            category_orders={'Servico': passeios_incluidos}
            )

        if st.session_state.base_luck == 'test_phoenix_salvador':

            texto_tag = '%{percent} - %{value:d} Paxs'

        else:

            texto_tag = '%{percent}'

        fig.update_traces(
            texttemplate=texto_tag, 
            hovertemplate='%{label}: %{value} Paxs'
            )

        fig.update_layout(
            showlegend=True, 
            margin=dict(
                t=50, 
                b=50, 
                l=50, 
                r=50
                )
            )

        return fig

    def plotar_graficos_pizza_todos_vendedores(row0, fig, fig_2=None):

        if not fig_2 is None:

            with row0[0]:

                st.plotly_chart(fig)

            with row0[1]:

                st.plotly_chart(fig_2)

        else:

            st.plotly_chart(fig)

    df_todos_vendedores_filtrado = gerar_df_todos_vendedores_filtrado(df_cont_passeio, st.session_state.passeios_incluidos)

    if st.session_state.base_luck == 'test_phoenix_natal':

        df_servicos_casa_vs_terceiros = gerar_df_servicos_casa_vs_terceiros(df_cont_passeio, ['Serviços Terceiros'])

        fig_2 = gerar_grafico_pizza_servicos_terceiros_da_casa_todos_vendedores(df_servicos_casa_vs_terceiros)

    if not df_todos_vendedores_filtrado.empty:

        fig = gerar_grafico_pizza_todos_vendedores(df_todos_vendedores_filtrado, st.session_state.passeios_incluidos)

        if st.session_state.base_luck == 'test_phoenix_natal':

            plotar_graficos_pizza_todos_vendedores(row0, fig, fig_2)

        elif st.session_state.base_luck in ['test_phoenix_joao_pessoa', 'test_phoenix_salvador', 'test_phoenix_noronha']:

            plotar_graficos_pizza_todos_vendedores(row0, fig)

def plotar_todos_graficos_pizza_por_vendedor(df_cont_passeio, row0):

    def gerar_df_vendedor_filtrado(df_cont_passeio, passeios_incluidos, vendedor):

        df_vendedor_filtrado = df_cont_passeio[(df_cont_passeio['Vendedor'] == vendedor) & (df_cont_passeio['Servico'].isin(passeios_incluidos))]

        if st.session_state.base_luck=='test_phoenix_joao_pessoa':

            df_vendedor_filtrado['Servico'] = df_vendedor_filtrado['Servico'].replace({
                'EMBARCAÇAO - CATAMARÃ DO FORRÓ ': 'CATAMARÃ DO FORRÓ', 
                'INGRESSO - BY NIGHT ': 'BY NIGHT PARAHYBA OXENTE '
                }
            ) 

        return df_vendedor_filtrado

    def gerar_grafico_pizza_vendedor(df_vendedor_filtrado, vendedor, passeios_incluidos):

        fig = px.pie(
            df_vendedor_filtrado, 
            names='Servico', 
            values='Total Paxs', 
            title=f'Distribuição de Paxs por Passeio - {vendedor}', 
            category_orders={'Servico': passeios_incluidos}
            )

        fig.update_traces(
            texttemplate='%{percent}', 
            hovertemplate='%{label}: %{value} Paxs'
            )
        
        fig.update_layout(
            showlegend=True, 
            margin=dict(
                t=50, 
                b=50, 
                l=50, 
                r=50
                )
            )

        return fig

    def gerar_grafico_pizza_servicos_terceiros_da_casa_vendedor(df_vendedor_filtrado, vendedor):

        fig = px.pie(
            df_vendedor_filtrado, 
            names='Serviços Terceiros', 
            values='Total Paxs', 
            title=f'Distribuição de Paxs - Terceiros vs Da Casa - {vendedor}', 
            )

        fig.update_traces(texttemplate='%{percent}', hovertemplate='%{label}: %{value} Paxs')
        
        fig.update_layout(showlegend=True, margin=dict(t=50, b=50, l=50, r=50))

        return fig

    if st.session_state.base_luck in ['test_phoenix_joao_pessoa', 'test_phoenix_salvador', 'test_phoenix_noronha']:

        coluna = 0

        for vendedor in df_cont_passeio['Vendedor'].unique():

            df_vendedor_filtrado = gerar_df_vendedor_filtrado(df_cont_passeio, st.session_state.passeios_incluidos, vendedor)
            
            if not df_vendedor_filtrado.empty:

                fig = gerar_grafico_pizza_vendedor(df_vendedor_filtrado, vendedor, st.session_state.passeios_incluidos)

                with row0[coluna%2]:
            
                    st.plotly_chart(fig)    

                coluna+=1

    elif st.session_state.base_luck == 'test_phoenix_natal':

        df_servicos_casa_vs_terceiros = gerar_df_servicos_casa_vs_terceiros(df_cont_passeio, ['Vendedor', 'Serviços Terceiros'])

        coluna = 0

        for vendedor in df_cont_passeio['Vendedor'].unique():

            df_vendedor_filtrado = gerar_df_vendedor_filtrado(df_cont_passeio, st.session_state.passeios_incluidos, vendedor)

            df_servicos_casa_vs_terceiros_vendedor = df_servicos_casa_vs_terceiros[df_servicos_casa_vs_terceiros['Vendedor']==vendedor]
            
            if not df_vendedor_filtrado.empty:

                fig = gerar_grafico_pizza_vendedor(df_vendedor_filtrado, vendedor, st.session_state.passeios_incluidos)

                fig_2 = gerar_grafico_pizza_servicos_terceiros_da_casa_vendedor(df_servicos_casa_vs_terceiros_vendedor, vendedor)

                with row0[coluna%2]:
            
                    st.plotly_chart(fig)

                coluna+=1

                with row0[coluna%2]:
            
                    st.plotly_chart(fig_2)

                coluna+=1

def gerar_grafico_todos_setores(df_setor_agrupado, titulo=None):

    if not titulo is None:

        nome_titulo = titulo

    else:

        nome_titulo = 'Valor Total por Setor'

    fig = px.bar(
        x=df_setor_agrupado['Setor'],  
        y=df_setor_agrupado['Venda_Filtrada'], 
        color=df_setor_agrupado['Setor'], 
        title=nome_titulo, 
        labels={'Venda_Filtrada': 'Valor Total', 'Setor': 'Setores'}, 
        text=df_setor_agrupado['Venda_Filtrada'].apply(formatar_moeda),
        )
    
    fig.update_traces(
        textposition='outside', 
        textfont=dict(size=10)
        )

    fig.update_layout(
        yaxis_title='Valor Total',
        xaxis_title='Setores'
        )

    return fig

def gerar_grafico_pizza_todos_setores(df_vendas_agrupado, titulo=None):

    if not titulo is None:

        nome_titulo = titulo

    else:

        nome_titulo = 'Participação % por Setor'

    fig = px.pie(
        df_vendas_agrupado, 
        names='Setor', 
        values='Venda_Filtrada', 
        title=nome_titulo,
        category_orders={'Setor': sorted(df_vendas_agrupado['Setor'].unique())}
        )

    fig.update_traces(
        texttemplate='%{percent}'
        )

    fig.update_layout(
        showlegend=True, 
        margin=dict(
            t=50, 
            b=50, 
            l=50, 
            r=50
            )
        )

    return fig

def gerar_grafico_setor_especifico(df_vendas_agrupado):

    max_venda = df_vendas_agrupado['Venda_Filtrada'].max() * 2

    max_tm = df_vendas_agrupado['Ticket_Medio'].max()*1.1

    fig = px.bar(
        x=df_vendas_agrupado['Vendedor'], 
        y=df_vendas_agrupado['Venda_Filtrada'], 
        color=df_vendas_agrupado['Vendedor'], 
        title='Valor Total por Vendedor', 
        labels={'Venda_Filtrada': 'Valor Total', 'Vendedor': 'Vendedores'}, 
        text=df_vendas_agrupado['Venda_Filtrada'].apply(formatar_moeda)
        )
    
    fig.update_traces(
        textposition='outside', 
        textfont=dict(size=10)
        )

    fig.update_layout(
        yaxis_title='Valor Total', 
        xaxis_title='Vendedores', 
        yaxis=dict(range=[0, max_venda]), 
        yaxis2=dict(
            title="Ticket Médio", 
            overlaying="y", 
            side="right", 
            showgrid=False, 
            range=[0, max_tm]
            )
        )

    fig.add_trace(go.Scatter(
        x=df_vendas_agrupado['Vendedor'], 
        y=df_vendas_agrupado['Ticket_Medio'], 
        mode='lines+markers+text', 
        name='Ticket Médio', 
        line=dict(width=1), 
        marker=dict(size=4), 
        yaxis='y2', 
        line_shape='spline', 
        text=df_vendas_agrupado['Ticket_Medio'].apply(formatar_moeda), 
        textposition='top center', 
        textfont=dict(size=10)
        )
        )

    return fig

def ajustar_valor_venda_servicos_guias_com_adicional(df_vendas):

    df_vendas = df_vendas.merge(st.session_state.df_custos_com_adicionais, on=['Servico', 'Adicional'], how='left')

    df_adicionais_relatorio = df_vendas[pd.notna(df_vendas['Adicional'])][['Servico', 'Adicional']].drop_duplicates().reset_index(drop=True)

    df_adicionais_relatorio['chave'] = df_adicionais_relatorio['Servico'] + ' - ' + df_adicionais_relatorio['Adicional']

    df_custos_com_adicionais_verificacao = st.session_state.df_custos_com_adicionais.copy()

    df_custos_com_adicionais_verificacao['chave'] = df_custos_com_adicionais_verificacao['Servico'] + ' - ' + df_custos_com_adicionais_verificacao['Adicional']

    adicionais_nao_tarifados = list(set(df_adicionais_relatorio['chave']) - set(df_custos_com_adicionais_verificacao['chave']))
    
    if len(adicionais_nao_tarifados)>0:

        st.warning(f"Os adicionais abaixo não têm custo cadastrado na aba de configurações da planilha base. Cadastre-os e atualize a página")

        df_adicionais_nao_tarifados = df_adicionais_relatorio[df_adicionais_relatorio['chave'].isin(adicionais_nao_tarifados)][['Servico', 'Adicional']]

        st.dataframe(df_adicionais_nao_tarifados, hide_index=True)

    mask_servicos_guias_com_adicional = (pd.notna(df_vendas['Adicional'])) & (df_vendas['Setor']=='Guia')

    df_vendas['Valor Adicional Total'] = df_vendas['Valor Adicional Adt']*df_vendas['Total_ADT']+df_vendas['Valor Adicional Chd']*df_vendas['Total_CHD']

    df_vendas.loc[mask_servicos_guias_com_adicional, 'Valor_Venda'] = df_vendas.loc[mask_servicos_guias_com_adicional, 'Valor_Venda'].fillna(0) - \
        df_vendas.loc[mask_servicos_guias_com_adicional, 'Valor Adicional Total'].fillna(0)

    return df_vendas

if __name__ == '__main__':
    
    st.set_page_config(layout='wide')

    if not 'base_luck' in st.session_state:
        
        base_fonte = st.query_params["base_luck"]

        st.session_state.lista_colunas_numero_df_metas_vendedor = ['Ano', 'Mes', 'Meta_Mes']

        st.session_state.lista_colunas_numero_df_config = ['Valor Parâmetro']

        st.session_state.lista_colunas_numero_df_vendas_manuais = ['Valor_Venda', 'Desconto_Global_Por_Servico', 'Total_ADT', 'Total_CHD']

        st.session_state.lista_colunas_data_df_vendas_manuais = ['Data_Venda']

        st.session_state.meses_disponiveis = {'Janeiro': 1, 'Fevereiro': 2, 'Março': 3, 'Abril': 4, 'Maio': 5, 'Junho': 6, 'Julho': 7, 'Agosto': 8, 'Setembro': 9, 'Outubro': 10, 'Novembro': 11, 
                                              'Dezembro': 12}
        
        st.session_state.meses_ingles_portugues = {'January': 'Janeiro', 'February': 'Fevereiro', 'March': 'Março', 'April': 'Abril', 'May': 'Maio', 'June': 'Junho', 'July': 'Julho', 
                                                   'August': 'Agosto', 'September': 'Setembro', 'October': 'Outubro', 'November': 'Novembro', 'December': 'Dezembro'}

        if base_fonte=='mcz':

            st.session_state.base_luck = 'test_phoenix_maceio'
            
        elif base_fonte=='rec':

            st.session_state.base_luck = 'test_phoenix_recife'

        elif base_fonte=='ssa':

            st.session_state.base_luck = 'test_phoenix_salvador'

            st.session_state.id_gsheet_metas_vendas = '1z5cCU_XFdixJqizHY5nKbX6inXBCnUGAAtWEg8rHAr0'

        elif base_fonte=='aju':

            st.session_state.base_luck = 'test_phoenix_aracaju'

        elif base_fonte=='fen':

            st.session_state.base_luck = 'test_phoenix_noronha'

            st.session_state.id_gsheet_metas_vendas = '13_3TZwLW2Q1Yt1JkWpq2wEEHLCTL9KbVvlx6zQd28Zo'

        elif base_fonte=='nat':

            st.session_state.base_luck = 'test_phoenix_natal'

            st.session_state.id_gsheet_metas_vendas = '11x9ht-Z73MpZJT0aySRDJOlfzpDCmOqOddkNLslzj84'

            st.session_state.lista_colunas_numero_df_ocupacao_hoteis = ['Ano', 'Mes', 'Paxs Hotel']

            st.session_state.lista_colunas_numero_df_custos_com_adicionais = ['Valor Adicional Adt', 'Valor Adicional Chd']

        elif base_fonte=='jpa':

            st.session_state.base_luck = 'test_phoenix_joao_pessoa'

            st.session_state.id_gsheet_metas_vendas = '1lM3FrBElaVfR-muyt8uFsxDUXOEaSXoPbUlHNJPdgaA'
            
            st.session_state.lista_colunas_numero_df_historico = ['Ano', 'Mes', 'Valor_Venda', 'Paxs ADT', 'Paxs CHD']
            
            st.session_state.lista_colunas_numero_df_historico_vendedor = ['Ano', 'Mes', 'Valor', 'Meta', 'Paxs_Total']

    st.title('Vendas Gerais por Setor')

    st.divider()

    if st.session_state.base_luck == 'test_phoenix_joao_pessoa':

        lista_keys_fora_do_session_state = [item for item in ['df_metas_vendedor', 'df_metas', 'df_config', 'df_vendas_final', 'df_guias_in', 'df_paxs_in'] if item not in st.session_state]

        if len(lista_keys_fora_do_session_state)>0:

            with st.spinner('Puxando metas de vendedores, metas de setores e configurações...'):

                if 'df_metas_vendedor' in lista_keys_fora_do_session_state:

                    gerar_df_metas_vendedor()

                if 'df_metas' in lista_keys_fora_do_session_state:

                    gerar_df_metas()

                if 'df_config' in lista_keys_fora_do_session_state:

                    puxar_df_config()

            with st.spinner('Puxando vendas, guias IN e paxs IN do Phoenix...'):

                if 'df_vendas_final' in lista_keys_fora_do_session_state:

                    st.session_state.df_vendas_final = gerar_df_vendas_final()

                if 'df_guias_in' in lista_keys_fora_do_session_state:

                    gerar_df_guias_in()

                if 'df_paxs_in' in lista_keys_fora_do_session_state:

                    gerar_df_paxs_in()

    elif st.session_state.base_luck == 'test_phoenix_natal':

        lista_keys_fora_do_session_state = [item for item in ['df_metas_vendedor', 'df_metas', 'df_ocupacao_hoteis', 'df_custos_com_adicionais', 'df_juntar_servicos', 'df_config', 'df_vendas_final',
                                                              'df_guias_in', 'df_paxs_in'] if item not in st.session_state]
        
        if len(lista_keys_fora_do_session_state)>0:

            with st.spinner('Puxando dados do Google Drive...'):

                if 'df_metas_vendedor' in lista_keys_fora_do_session_state:

                    gerar_df_metas_vendedor()

                if 'df_metas' in lista_keys_fora_do_session_state:

                    gerar_df_metas()

                if 'df_ocupacao_hoteis' in lista_keys_fora_do_session_state:

                    gerar_df_ocupacao_hoteis()

                if 'df_custos_com_adicionais' in lista_keys_fora_do_session_state:

                    gerar_df_custos_com_adicionais()

                if 'df_juntar_servicos' in lista_keys_fora_do_session_state:

                    puxar_aba_simples(st.session_state.id_gsheet_metas_vendas, 'Juntar Serviços', 'df_juntar_servicos')

                if 'df_config' in lista_keys_fora_do_session_state:

                    puxar_df_config()

            with st.spinner('Puxando dados do Phoenix...'):

                if 'df_vendas_final' in lista_keys_fora_do_session_state:

                    st.session_state.df_vendas_final = gerar_df_vendas_final()

                if 'df_guias_in' in lista_keys_fora_do_session_state:

                    gerar_df_guias_in()

                if 'df_paxs_in' in lista_keys_fora_do_session_state:

                    gerar_df_paxs_in()

    elif st.session_state.base_luck == 'test_phoenix_salvador':

        lista_keys_fora_do_session_state = [item for item in ['df_metas_vendedor', 'df_metas_vendedor_setor', 'df_metas_vendedor_canal_vendas', 'df_metas', 'df_juntar_servicos', 'df_config', 
                                                              'df_canal_de_vendas_setor', 'df_vendas_final', 'df_guias_in', 'df_paxs_in'] if item not in st.session_state]
        
        if len(lista_keys_fora_do_session_state)>0:

            with st.spinner('Puxando dados do Google Drive...'):

                if 'df_metas_vendedor' in lista_keys_fora_do_session_state:

                    gerar_df_metas_vendedor()

                if 'df_metas_vendedor_setor' in lista_keys_fora_do_session_state:

                    gerar_df_metas_vendedor_setor()

                if 'df_metas_vendedor_canal_vendas' in lista_keys_fora_do_session_state:

                    gerar_df_metas_vendedor_canal_vendas()

                if 'df_metas' in lista_keys_fora_do_session_state:

                    gerar_df_metas()

                if 'df_juntar_servicos' in lista_keys_fora_do_session_state:

                    puxar_aba_simples(st.session_state.id_gsheet_metas_vendas, 'Juntar Serviços', 'df_juntar_servicos')

                if 'df_config' in lista_keys_fora_do_session_state:

                    puxar_df_config()

                if 'df_canal_de_vendas_setor' in lista_keys_fora_do_session_state:

                    puxar_aba_simples(st.session_state.id_gsheet_metas_vendas, 'Canal de Vendas - Setor', 'df_canal_de_vendas_setor')

            with st.spinner('Puxando dados do Phoenix...'):

                if 'df_vendas_final' in lista_keys_fora_do_session_state:

                    st.session_state.df_vendas_final = gerar_df_vendas_final()

                if 'df_guias_in' in lista_keys_fora_do_session_state:

                    gerar_df_guias_in()

                if 'df_paxs_in' in lista_keys_fora_do_session_state:

                    gerar_df_paxs_in()

    elif st.session_state.base_luck == 'test_phoenix_noronha':

        lista_keys_fora_do_session_state = [item for item in ['df_metas_vendedor', 'df_metas', 'df_config', 'df_vendas_final', 'df_guias_in', 'df_paxs_in'] if item not in st.session_state]
        
        if len(lista_keys_fora_do_session_state)>0:

            with st.spinner('Puxando dados do Google Drive...'):

                if 'df_metas_vendedor' in lista_keys_fora_do_session_state:

                    gerar_df_metas_vendedor()

                if 'df_metas' in lista_keys_fora_do_session_state:

                    gerar_df_metas()

                if 'df_config' in lista_keys_fora_do_session_state:

                    puxar_df_config()

            with st.spinner('Puxando dados do Phoenix...'):

                if 'df_vendas_final' in lista_keys_fora_do_session_state:

                    st.session_state.df_vendas_final = gerar_df_vendas_final()

                if 'df_guias_in' in lista_keys_fora_do_session_state:

                    gerar_df_guias_in()

                if 'df_paxs_in' in lista_keys_fora_do_session_state:

                    gerar_df_paxs_in()

    lista_setor = gerar_lista_setor()

    row1 = st.columns(2)

    col1, col2, col3 = st.columns([1.5, 3.0, 4.50])

    row1 = st.columns(2)

    # Objetos Data Início, Data Fim e Setor

    data_ini, data_fim, mes_ano_ini, mes_ano_fim, seleciona_setor, base_tm = colher_periodo_e_setor(col1)

    if len(seleciona_setor)>0:

        # Filtrando datas selecionadas e juntando serviços com nomenclaturas diferentes

        if st.session_state.base_luck=='test_phoenix_natal':

            df_vendas, df_paxs_in, df_guias_in, df_metas_vendedor, df_metas_setor, df_ocupacao_hoteis = filtrar_periodo_dfs_base_luck(
                data_ini, 
                data_fim, 
                mes_ano_ini, 
                mes_ano_fim
            )

        elif st.session_state.base_luck in ['test_phoenix_joao_pessoa', 'test_phoenix_noronha']:

            df_vendas, df_paxs_in, df_guias_in, df_metas_vendedor, df_metas_setor = filtrar_periodo_dfs_base_luck(
                data_ini, 
                data_fim, 
                mes_ano_ini, 
                mes_ano_fim
            )

        elif st.session_state.base_luck == 'test_phoenix_salvador':

            df_vendas, df_paxs_in, df_guias_in, df_metas_vendedor, df_metas_setor, df_metas_vendedor_setor, df_metas_vendedor_canal_vendas = filtrar_periodo_dfs_base_luck(
                data_ini, 
                data_fim, 
                mes_ano_ini, 
                mes_ano_fim
            )

        df_guias_in = df_guias_in.groupby('Guia', as_index=False)['Total_Paxs'].sum()

        # Filtrando setores selecionados

        df_vendas = filtrar_setores_selecionados(seleciona_setor, df_vendas)

        if len(df_vendas)>0:

            # Colhe as escolhas do usuário

            seleciona_canal, seleciona_vend, seleciona_hotel, filtrar_servicos_terceiros = colher_selecoes_vendedor_canal_hotel(
                df_vendas, 
                col1
            )

            # Filtra os dados de vendas de acordo com as escolhas do usuário

            df_vendas = filtrar_canal_vendedor_hotel_df_vendas(
                df_vendas, 
                seleciona_canal, 
                seleciona_vend, 
                seleciona_hotel, 
                filtrar_servicos_terceiros
            )

            # Gera dataframe de vendas por hotel

            df_hotel = gerar_df_hotel(df_vendas)

            # Ajuste de coluna Desconto Global, porque em João Pessoa tem a questão do EXTRA

            df_vendas = ajustar_desconto_global(df_vendas)

            # Gerando o dataframe das vendas agrupadas

            if st.session_state.base_luck=='test_phoenix_natal':

                df_vendas = ajustar_valor_venda_servicos_guias_com_adicional(df_vendas)

                df_vendas_agrupado = gerar_df_vendas_agrupado(
                    df_vendas, 
                    df_metas_vendedor, 
                    df_guias_in, 
                    df_paxs_in, 
                    df_metas_setor, 
                    df_ocupacao_hoteis
                )

            elif st.session_state.base_luck == 'test_phoenix_salvador':

                df_vendas_agrupado = gerar_df_vendas_agrupado(
                    df_vendas, 
                    df_metas_vendedor, 
                    df_guias_in, 
                    df_paxs_in, 
                    df_metas_setor, 
                    df_metas_vendedor_setor=df_metas_vendedor_setor
                )

            else:

                df_vendas_agrupado = gerar_df_vendas_agrupado(
                    df_vendas, 
                    df_metas_vendedor, 
                    df_guias_in, 
                    df_paxs_in, 
                    df_metas_setor
                )

            df_cont_passeio = df_vendas.groupby(['Vendedor', 'Servico'], as_index=False)['Total Paxs'].sum()

            soma_vendas, tm_vendas, tm_setor_estip, total_desconto, paxs_recebidos, med_desconto, meta_esperada_formatada, perc_alcancado = \
                gerar_soma_vendas_tm_vendas_desconto_paxs_recebidos(df_vendas_agrupado)
            
            plotar_todos_quadrados_html(
                col2, 
                soma_vendas, 
                meta_esperada_formatada, 
                tm_setor_estip, 
                total_desconto, 
                perc_alcancado, 
                paxs_recebidos, 
                tm_vendas, 
                med_desconto
            )

            with col3:

                i=0

                df_estilizado = gerar_df_estilizado(df_vendas_agrupado)

                plotar_vendas_por_vendedor(df_estilizado)

                if st.session_state.base_luck == 'test_phoenix_salvador':

                    i = plotar_vendas_por_vendedor_setor_canal_vendas(
                        seleciona_setor, 
                        df_vendas_agrupado, 
                        row1, 
                        i, 
                        df_vendas, 
                        df_paxs_in, 
                        df_metas_vendedor, 
                        df_metas_setor, 
                        df_metas_vendedor_canal_vendas
                    )

                elif st.session_state.base_luck in ['test_phoenix_joao_pessoa', 'test_phoenix_noronha']:

                    i = plotar_vendas_por_vendedor_setor_canal_vendas(
                        seleciona_setor, 
                        df_vendas_agrupado, 
                        row1, 
                        i, 
                        df_vendas, 
                        df_paxs_in, 
                        df_metas_vendedor, 
                        df_metas_setor
                    )

                i = plotar_vendas_por_hotel(row1, i, df_hotel)
                
                if len(seleciona_setor)==1 and seleciona_setor[0] == '--- Todos ---':

                    df_setor_agrupado = df_vendas_agrupado[pd.notna(df_vendas_agrupado['Setor'])].groupby('Setor', as_index=False)['Venda_Filtrada'].sum()

                    if not df_setor_agrupado.empty:
                    
                        i = plotar_grafico_todos_setores(row1, i, df_setor_agrupado)

                        if st.session_state.base_luck == 'test_phoenix_salvador':
                        
                            i = plotar_grafico_pizza_todos_setores(row1, i, df_setor_agrupado)

                            df_vendas_vo_cv = gerar_df_vendas_vo_cv(df_vendas)

                            i = plotar_grafico_todos_setores_vo_destrinchado(df_setor_agrupado, df_vendas_vo_cv, row1, i)

                else:

                    if len(seleciona_setor)==1 and seleciona_setor[0]=='Guia':

                        i = plotar_grafico_setor_guia(df_vendas_agrupado, row1, i)

                    elif len(seleciona_setor)==1 and seleciona_setor[0]=='Vendas Online':

                        i = plotar_grafico_setor_vendas_online(df_vendas_agrupado, row1, i, df_vendas) 

                    else:

                        i = plotar_grafico_setor_especifico(df_vendas_agrupado, row1, i)

        else:

            st.error('O setor selecionado não possui venda nesse período')

    row0 = st.columns(2)

    if len(seleciona_setor)==1 and seleciona_setor[0] == '--- Todos ---':

        plotar_todos_graficos_pizza_todos_setores(df_cont_passeio, row0)

    elif len(seleciona_setor)>0 and len(df_vendas)>0:

        plotar_todos_graficos_pizza_por_vendedor(df_cont_passeio, row0)
