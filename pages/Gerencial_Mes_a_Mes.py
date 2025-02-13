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

def gerar_df_vendas():

    request_select = '''
        SELECT 
            Canal_de_Vendas,
            Vendedor,
            Nome_Segundo_Vendedor,
            Status_Financeiro,
            Data_Venda,
            Valor_Venda,
            Nome_Estabelecimento_Origem,
            Desconto_Global_Por_Servico,
            Desconto_Global,
            Nome_Parceiro,
            Cod_Reserva,
            Nome_Servico,
            `Total ADT`,
            `Total CHD`
        FROM vw_sales
        '''
    
    st.session_state.df_vendas = gerar_df_phoenix(st.session_state.base_luck, request_select)

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

def gerar_df_vendas_manuais():

    puxar_aba_simples(st.session_state.id_gsheet_metas_vendas, 'BD - Vendas Manuais', 'df_vendas_manuais')

    tratar_colunas_numero_df(st.session_state.df_vendas_manuais, st.session_state.lista_colunas_numero_df_vendas_manuais)

    tratar_colunas_data_df(st.session_state.df_vendas_manuais, st.session_state.lista_colunas_data_df_vendas_manuais)

def gerar_df_reembolsos():

    puxar_aba_simples(st.session_state.id_gsheet_reembolsos, 'BD - Geral', 'df_reembolsos')

    tratar_colunas_numero_df(st.session_state.df_reembolsos, st.session_state.lista_colunas_numero_df_reembolsos)

    tratar_colunas_data_df(st.session_state.df_reembolsos, st.session_state.lista_colunas_data_df_reembolsos)

    st.session_state.df_reembolsos['Ano'] = pd.to_datetime(st.session_state.df_reembolsos['Data_venc']).dt.year
    
    st.session_state.df_reembolsos['Mes'] = pd.to_datetime(st.session_state.df_reembolsos['Data_venc']).dt.month

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

def gerar_df_vendas_final():

    df_vendas = pd.concat([st.session_state.df_vendas, st.session_state.df_vendas_manuais], ignore_index=True)

    df_vendas['Data_Venda'] = pd.to_datetime(df_vendas['Data_Venda']).dt.date

    df_vendas = df_vendas[~df_vendas['Status_Financeiro'].isin(['TROCADO', 'A Faturar'])]

    df_vendas = ajustar_nomes_leticia_soraya(df_vendas)

    df_vendas = ajustar_pdvs_facebook(df_vendas)

    df_vendas['Total Paxs'] = df_vendas['Total ADT'] + df_vendas['Total CHD'] / 2

    df_vendas['Total Paxs'] = df_vendas['Total Paxs'].fillna(0)

    df_vendas['Ano'] = pd.to_datetime(df_vendas['Data_Venda']).dt.year

    df_vendas['Mes'] = pd.to_datetime(df_vendas['Data_Venda']).dt.month

    df_vendas['Mes_Ano'] = pd.to_datetime(df_vendas['Data_Venda']).dt.to_period('M')

    df_vendas['Setor'] = df_vendas['Vendedor'].str.split(' - ').str[1].replace({'OPERACIONAL':'LOGISTICA', 'BASE AEROPORTO ': 'LOGISTICA', 'BASE AEROPORTO': 'LOGISTICA', 'COORD. ESCALA': 'LOGISTICA', 
                                                                                'KUARA/MANSEAR': 'LOGISTICA', 'MOTORISTA': 'LOGISTICA', 'SUP. LOGISTICA': 'LOGISTICA'})
    
    dict_setor_meta = {'GUIA': 'Meta_Guia', 'PDV': 'Meta_PDV', 'HOTEL VENDAS': 'Meta_HV', 'GRUPOS': 'Meta_Grupos', 'VENDAS ONLINE': 'Meta_VendasOnline'}

    df_metas_indexed = st.session_state.df_metas.set_index('Mes_Ano')

    df_vendas['Meta'] = df_vendas.apply(lambda row: df_metas_indexed.at[row['Mes_Ano'], dict_setor_meta[row['Setor']]] 
                                        if row['Setor'] in dict_setor_meta and row['Mes_Ano'] in df_metas_indexed.index else 0, axis=1)

    return df_vendas

def gerar_df_ranking():

    request_select = '''
        SELECT 
            `1 Vendedor`,
            `Data de Execucao`,
            `Tipo de Servico`,
            `Servico`,
            `Total ADT`,
            `Total CHD`,
            `Codigo da Reserva`
        FROM vw_sales_ranking
        WHERE `TIPO DE SERVICO` = 'TOUR';
        '''
    
    st.session_state.df_ranking = gerar_df_phoenix(st.session_state.base_luck, request_select)

    st.session_state.df_ranking['Data de Execucao'] = pd.to_datetime(st.session_state.df_ranking['Data de Execucao']).dt.date

    st.session_state.df_ranking['Ano'] = pd.to_datetime(st.session_state.df_ranking['Data de Execucao']).dt.year
    
    st.session_state.df_ranking['Mes'] = pd.to_datetime(st.session_state.df_ranking['Data de Execucao']).dt.month
    
    st.session_state.df_ranking['Mes_Ano'] = pd.to_datetime(st.session_state.df_ranking['Data de Execucao']).dt.to_period('M')

    st.session_state.df_ranking['Setor'] = st.session_state.df_ranking['1 Vendedor'].str.split(' - ').str[1].replace({'OPERACIONAL':'LOGISTICA', 'BASE AEROPORTO ': 'LOGISTICA', 
                                                                                                                      'BASE AEROPORTO': 'LOGISTICA', 'COORD. ESCALA': 'LOGISTICA', 
                                                                                                                      'KUARA/MANSEAR': 'LOGISTICA'})
    
    st.session_state.df_ranking['Total Paxs'] = st.session_state.df_ranking['Total ADT'] + st.session_state.df_ranking['Total CHD'] / 2

def gerar_df_paxs_in():

    request_select = '''
        SELECT 
            `Tipo de Servico`,
            `Data Execucao`,
            `Servico`,
            `Status do Servico`,
            `Total ADT`,
            `Total CHD`
        FROM vw_router
        WHERE
            `Servico` != 'GUIA BASE NOTURNO' and
            `Servico` != 'AEROPORTO JOÃO PESSOA / HOTÉIS PITIMBU' and
            `Servico` != 'AEROPORTO JOÃO PESSOA / HOTÉIS CAMPINA GRANDE' and
            `Servico` != 'FAZER CONTATO - SEM TRF IN ' and
            `Servico` != 'AEROPORTO CAMPINA GRANDE / HOTEL CAMPINA GRANDE ' and
            `Servico` != 'GUIA BASE DIURNO '; 
        '''
    
    st.session_state.df_paxs_in = gerar_df_phoenix(st.session_state.base_luck, request_select)

    st.session_state.df_paxs_in['Data Execucao'] = pd.to_datetime(st.session_state.df_paxs_in['Data Execucao']).dt.date

    st.session_state.df_paxs_in = st.session_state.df_paxs_in[(st.session_state.df_paxs_in['Tipo de Servico']=='IN') & (st.session_state.df_paxs_in['Status do Servico']!='CANCELADO')]\
        .reset_index(drop=True)
    
    st.session_state.df_paxs_in['Ano'] = pd.to_datetime(st.session_state.df_paxs_in['Data Execucao']).dt.year
    
    st.session_state.df_paxs_in['Mes'] = pd.to_datetime(st.session_state.df_paxs_in['Data Execucao']).dt.month
    
    st.session_state.df_paxs_in['Mes_Ano'] = pd.to_datetime(st.session_state.df_paxs_in['Data Execucao']).dt.to_period('M')

    st.session_state.df_paxs_in['Total_Paxs_Periodo'] = st.session_state.df_paxs_in['Total ADT'] + (st.session_state.df_paxs_in['Total CHD'] / 2)

    st.session_state.df_paxs_in = pd.merge(st.session_state.df_paxs_in, st.session_state.df_metas[['Mes_Ano', 'Paxs_Desc']], on='Mes_Ano', how='left')

    st.session_state.df_paxs_in['Paxs_Desc'] = pd.to_numeric(st.session_state.df_paxs_in['Paxs_Desc'], errors='coerce')

def gerar_df_metas():

    puxar_aba_simples(st.session_state.id_gsheet_metas_vendas, 'BD - Metas', 'df_metas')

    tratar_colunas_numero_df(st.session_state.df_metas, st.session_state.lista_colunas_numero_df_metas)

    tratar_colunas_data_df(st.session_state.df_metas, st.session_state.lista_colunas_data_df_metas)

    st.session_state.df_metas['Mes_Ano'] = pd.to_datetime(st.session_state.df_metas['Data']).dt.to_period('M')

def gerar_df_historico():

    puxar_aba_simples(st.session_state.id_gsheet_metas_vendas, 'BD - Historico', 'df_historico')

    tratar_colunas_numero_df(st.session_state.df_historico, st.session_state.lista_colunas_numero_df_historico)

    tratar_colunas_data_df(st.session_state.df_historico, st.session_state.lista_colunas_data_df_historico)

    st.session_state.df_historico['Ano'] = pd.to_datetime(st.session_state.df_historico['Data']).dt.year
    
    st.session_state.df_historico['Mes'] = pd.to_datetime(st.session_state.df_historico['Data']).dt.month
    
    st.session_state.df_historico['Mes_Ano'] = pd.to_datetime(st.session_state.df_historico['Data']).dt.to_period('M')

def filtrar_periodo_dfs():

    df_vendas = st.session_state.df_vendas_final[(st.session_state.df_vendas_final['Ano'].isin(ano_selecao)) & 
                                                 (st.session_state.df_vendas_final['Mes'].isin(st.session_state.mes_selecao_valores))].reset_index(drop=True)

    df_paxs_in = st.session_state.df_paxs_in[(st.session_state.df_paxs_in['Ano'].isin(ano_selecao)) & 
                                             (st.session_state.df_paxs_in['Mes'].isin(st.session_state.mes_selecao_valores))].reset_index(drop=True)
    
    df_reembolsos = st.session_state.df_reembolsos[(st.session_state.df_reembolsos['Ano'].isin(ano_selecao)) & 
                                                   (st.session_state.df_reembolsos['Mes'].isin(st.session_state.mes_selecao_valores))].reset_index(drop=True)
    
    df_historico = st.session_state.df_historico[(st.session_state.df_historico['Ano'].isin(ano_selecao)) & 
                                                 (st.session_state.df_historico['Mes'].isin(st.session_state.mes_selecao_valores))].reset_index(drop=True)
    
    df_ranking = st.session_state.df_ranking[(st.session_state.df_ranking['Ano'].isin(ano_selecao)) & 
                                             (st.session_state.df_ranking['Mes'].isin(st.session_state.mes_selecao_valores))].reset_index(drop=True)

    return df_vendas, df_paxs_in, df_reembolsos, df_historico, df_ranking

def adicionar_total_paxs_periodo_vendas(df_paxs_in, df_vendas):

    soma_paxs = df_paxs_in['Total_Paxs_Periodo'].sum()

    desc_paxs = df_paxs_in['Paxs_Desc'].mean()

    df_vendas['Total_Paxs'] = float(soma_paxs) + float(desc_paxs)

    return df_vendas

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

def ajuste_colunas_float(df_vendas):

    for coluna in ['Valor_Venda', 'Total_Paxs']:

        df_vendas[coluna] = df_vendas[coluna].fillna(0).astype(float)

    return df_vendas

def gerar_df_contador(df_vendas):

    df_cont_servicos = df_vendas.groupby('Vendedor')['Nome_Servico'].count().reset_index()

    df_cont_reservas = df_vendas.groupby('Vendedor')['Cod_Reserva'].nunique().reset_index()

    df_cont_servicos.columns = ['Vendedor', 'Quantidade_Servicos']

    df_cont_reservas.columns = ['Vendedor', 'Quantidade_Reservas']

    df_contador = df_cont_servicos.merge(df_cont_reservas[['Vendedor', 'Quantidade_Reservas']], on='Vendedor', how='left')

    return df_contador

def ajustar_desconto_global(df_vendas):

    df_vendas['Desconto_Global_Ajustado'] = np.where((df_vendas['Desconto_Global_Por_Servico'].notna()) & (df_vendas['Desconto_Global_Por_Servico'] < 1000) & 
                                                     (df_vendas['Nome_Servico'] != 'EXTRA'), df_vendas['Desconto_Global_Por_Servico'], 0)
    
    return df_vendas

def gerar_df_vendas_agrupado(df_vendas, df_reembolsos, df_contador, df_metas_vendedor):

    df_vendas_agrupado = df_vendas.groupby(['Vendedor', 'Setor'], dropna=False).agg({'Valor_Venda': 'sum', 'Total_Paxs': 'mean', 'Desconto_Global_Ajustado': 'sum', 'Paxs_IN': 'mean', 
                                                                                     'Meta': 'mean'}).reset_index()

    df_vendas_agrupado = pd.merge(df_vendas_agrupado, df_reembolsos, on='Vendedor', how='left')

    df_vendas_agrupado['Venda_Filtrada'] = df_vendas_agrupado['Valor_Venda'] - df_vendas_agrupado['Valor_Total'].fillna(0)

    df_vendas_agrupado['Venda_por_Reserva'] = df_contador['Quantidade_Servicos'] / df_contador['Quantidade_Reservas']

    df_vendas_agrupado['Ticket_Medio'] = np.where(df_vendas_agrupado['Setor'] == 'GUIA', df_vendas_agrupado['Venda_Filtrada'] / df_vendas_agrupado['Paxs_IN'], 
                                                  df_vendas_agrupado['Venda_Filtrada'] / df_vendas_agrupado['Total_Paxs'])
    
    df_vendas_agrupado['Ticket_Medio'] = df_vendas_agrupado['Ticket_Medio'].fillna(df_vendas_agrupado['Venda_Filtrada'])
    
    df_vendas_agrupado = df_vendas_agrupado.sort_values(by='Venda_Filtrada', ascending=False)

    df_vendas_agrupado = df_vendas_agrupado.merge(df_metas_vendedor[['Vendedor', 'Setor', 'Meta_Mes']], on=['Vendedor', 'Setor'], how='left')

    return df_vendas_agrupado

def formatar_moeda(valor):

    return format_currency(valor, 'BRL', locale='pt_BR')

def gerar_lista_setor():

    lista_setor = sorted(st.session_state.df_vendas_final['Setor'].str.strip().dropna().unique().tolist())

    lista_setor.insert(0, '--- Todos ---')

    lista_setor = [item for item in lista_setor if item not in ['COORD. FINANCEIRO', 'COORD. VENDAS', 'LOGISTICA', 'SAC', 'GUIA TOUR AZUL', 'PLANEJAMENTO ESTRATÉGICO', 'COMERCIAL', 'DIRETORA', 
                                                                'SUP. EXPERIÊNCIA AO CLIENTE/SAC']]

    return lista_setor

def plotar_quadrados_html(titulo, info_numero):
    
    st.markdown(f"""
    <div style="background-color:#f0f0f5; border-radius:10px; border: 2px solid #ccc; text-align: center; width: 180px; margin:0 auto; margin: 0 auto 10px auto; min-height: 50px;">
        <h3 style="color: #333; font-size: 18px; padding: 0px 10px; text-align: center; margin-bottom: 0px; ">{titulo}</h3>
        <h2 style="color: #047c6c; font-size: 20px; padding: 10px 30px; text-align: center; margin:0 auto; white-space: nowrap;">{info_numero}</h2>
    </div>
    """, unsafe_allow_html=True)

def gerar_meta_esperada_perc_alcancado_todos_setores(df_vendas_agrupado, soma_vendas):

    df_meta_setor = df_vendas_agrupado.groupby('Setor', as_index=False).agg({'Total_Paxs': 'mean', 'Meta': 'first'})

    df_meta_setor['Meta_Esperada'] = df_meta_setor['Total_Paxs'] * df_meta_setor['Meta']

    meta_esperada_total = df_meta_setor['Meta_Esperada'].sum()

    if meta_esperada_total == 0:

        meta_esperada_formatada = '-- Sem Meta --'

        perc_alcancado = f'{round((soma_vendas / meta_esperada_total) * 100, 2)}%'

    else:

        meta_esperada_formatada = formatar_moeda(meta_esperada_total)

        perc_alcancado = f'{round((soma_vendas / meta_esperada_total) * 100, 2)}%'

    return meta_esperada_formatada, perc_alcancado

def gerar_meta_esperada_perc_alcancado_setor_especifico(df_vendas_agrupado, soma_vendas):
                
    meta_esperada = df_vendas_agrupado['Total_Paxs'] * df_vendas_agrupado['Meta'] 

    meta_esperada = meta_esperada.fillna(0)

    if meta_esperada.mean() == 0:

        meta_esperada_formatada = "-- Sem Meta --"

        perc_alcancado = "-- Sem Meta --"

    else:

        meta_esperada_formatada = formatar_moeda(meta_esperada.mean())

        perc_alcancado = f'{round((soma_vendas / meta_esperada.mean()) * 100, 2)}%'

    return meta_esperada_formatada, perc_alcancado

def gerar_media_descontos(total_desconto, soma_vendas):

    if float(total_desconto) != 0 or float(soma_vendas) != 0:

        divisor_desconto = float(total_desconto) / float(soma_vendas)

        if divisor_desconto != 0:

            Med_desconto = (float(total_desconto) / (float(soma_vendas) + float(total_desconto))) * 100

            return f'{round(Med_desconto, 2)}%'

        else:

            return '0%'  
    else:

        return '0%'
    
def gerar_soma_vendas_tm_vendas_desconto_paxs_recebidos(df_vendas_agrupado, df_vendas):

    soma_vendas = df_vendas_agrupado['Venda_Filtrada'].sum()

    tm_vendas = soma_vendas / df_vendas_agrupado['Total_Paxs'].mean()

    tm_setor_estip = formatar_moeda(df_vendas_agrupado['Meta'].mean())

    total_desconto = df_vendas[df_vendas['Nome_Servico'] != 'EXTRA']['Desconto_Global_Ajustado'].sum()

    if pd.isna(df_vendas_agrupado['Total_Paxs'].fillna(0).mean()):

        paxs_recebidos = "-"

    else:

        paxs_recebidos = str(int(df_vendas_agrupado['Total_Paxs'].fillna(0).mean()))

    med_desconto = gerar_media_descontos(total_desconto, soma_vendas)

    return soma_vendas, tm_vendas, tm_setor_estip, total_desconto, paxs_recebidos, med_desconto

def highlight_ticket(row):

    if row['Ticket_Medio'] > row['Meta_Mes']:

        return ['background-color: lightgreen'] * len(row)
    
    else:

        return [''] * len(row)

def gerar_df_estilizado(df_vendas_agrupado):
    
    df_estilizado = df_vendas_agrupado[['Vendedor', 'Venda_Filtrada', 'Ticket_Medio', 'Meta_Mes', 'Venda_por_Reserva', 'Desconto_Global_Ajustado']].copy()

    df_estilizado = df_estilizado.dropna(subset=['Desconto_Global_Ajustado', 'Venda_Filtrada'])

    for coluna in ['Desconto_Global_Ajustado', 'Venda_Filtrada']:

        df_estilizado[coluna] = pd.to_numeric(df_estilizado[coluna], errors='coerce')

    df_estilizado['% Desconto'] = (df_estilizado['Desconto_Global_Ajustado'] / (df_estilizado['Venda_Filtrada'] + df_estilizado['Desconto_Global_Ajustado']))*100

    df_estilizado['Meta_Mes'] = df_estilizado['Meta_Mes'].replace(0, None).fillna(df_vendas_agrupado['Meta'])

    df_estilizado = df_estilizado.rename(columns={'Venda_por_Reserva': 'Venda_Reser', 'Desconto_Global_Ajustado': 'Total_Descontos', 'Venda_Filtrada': 'Valor_Vendas'})

    df_estilizado = df_estilizado.drop_duplicates(keep='last')

    df_estilizado = df_estilizado.style.apply(highlight_ticket, axis=1)

    df_estilizado = df_estilizado.format({'Valor_Vendas': formatar_moeda, 'Ticket_Medio': formatar_moeda, 'Meta_Mes': formatar_moeda, 'Venda_Reser': '{:.2f}'.format, 
                                          'Total_Descontos': formatar_moeda, '% Desconto':'{:.2f}%'.format})
    
    return df_estilizado

def gerar_grafico_todos_setores(df_setor_agrupado):

    fig = px.bar(
            x=df_setor_agrupado['Setor'], 
            y=df_setor_agrupado['Venda_Filtrada'], 
            color=df_setor_agrupado['Setor'],
            title='Valor Total por Setor',
            labels={'Venda_Filtrada': 'Valor Total', 'Setor': 'Setores'},
            text=df_setor_agrupado['Venda_Filtrada'].apply(formatar_moeda),
            color_discrete_sequence=['#047c6c']  
    )
    fig.update_traces(
        textposition='outside',
        textfont=dict(size=10, color='green')
    )
    fig.update_layout(
        yaxis_title='Valor Total',
        xaxis_title='Setores'
    )

    return fig

def gerar_grafico_setor_especifico(df_vendas_agrupado):

    fig = px.bar(
                x=df_vendas_agrupado['Vendedor'], 
                y=df_vendas_agrupado['Venda_Filtrada'], 
                color=df_vendas_agrupado['Setor'],  # Adiciona cores diferentes para cada setor
                title='Valor Total por Vendedor',
                labels={'Venda_Filtrada': 'Valor Total', 'Vendedor': 'Vendedores'},
                text=df_vendas_agrupado['Venda_Filtrada'].apply(formatar_moeda),
                color_discrete_sequence=['#047c6c']  
                )
    fig.update_traces(
        textposition='outside',
        textfont=dict(size=10, color='green')
        )
    fig.update_layout(
        yaxis_title='Valor Total',
        xaxis_title='Vendedores',
        yaxis2=dict(
        title="Ticket Médio",
        overlaying="y",
        side="right",
        showgrid=False
        )
        )

    fig.add_trace(
        go.Scatter(
            x=df_vendas_agrupado['Vendedor'],
            y=df_vendas_agrupado['Ticket_Medio'],  # Garante que o tipo seja float para a linha
            mode='lines+markers+text',
            name='Ticket Médio',
            line=dict(color='orange', width=1),  # Cor e largura da linha
            marker=dict(size=4),
            yaxis='y2',
            line_shape='spline',  # Suaviza a linha
            #smoothing=1.3  # 
            text=df_vendas_agrupado['Ticket_Medio'].apply(formatar_moeda),
            textposition='top center',
            textfont=dict(size=10, color='orange')
        )
    )

    return fig

def gerar_grafico_sem_dados():

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=['Nenhum Dado'],
        y=[0],
        text='Sem Dados',
        textposition='inside'
    ))
    fig.update_layout(
        title='Nenhum Dado Disponível',
        xaxis_title='Vendedores',
        yaxis_title='Valor Total'
    )

    return fig

def gerar_df_todos_vendedores_filtrado(df_cont_passeio, passeios_incluidos):

    df_todos_vendedores_filtrado = df_cont_passeio[df_cont_passeio['Nome_Servico'].isin(passeios_incluidos)]

    soma_catamara = df_todos_vendedores_filtrado.loc[df_todos_vendedores_filtrado['Nome_Servico'].isin(['CATAMARÃ DO FORRÓ', 'EMBARCAÇAO - CATAMARÃ DO FORRÓ ']), 'Total Paxs'].sum()

    soma_bynight = df_todos_vendedores_filtrado.loc[df_todos_vendedores_filtrado['Nome_Servico'].isin(['BY NIGHT PARAHYBA OXENTE ', 'INGRESSO - BY NIGHT ']), 'Total Paxs'].sum()

    df_todos_vendedores_filtrado.loc[df_todos_vendedores_filtrado['Nome_Servico'] == 'BY NIGHT PARAHYBA OXENTE ', 'Total Paxs'] = soma_bynight

    df_todos_vendedores_filtrado.loc[df_todos_vendedores_filtrado['Nome_Servico'] == 'CATAMARÃ DO FORRÓ', 'Total Paxs'] = soma_catamara

    return df_todos_vendedores_filtrado

def gerar_grafico_pizza_todos_vendedores(df_todos_vendedores_filtrado, passeios_incluidos):

    fig = px.pie(df_todos_vendedores_filtrado, names='Nome_Servico', 
        values='Total Paxs', 
        title='Distribuição de Paxs por Passeio',
        category_orders={'Nome_Servico': passeios_incluidos},
        #color_discrete_sequence=cor_base
    )
    fig.update_traces(
        texttemplate='%{percent}',  # Apenas a porcentagem dentro da fatia
        hovertemplate='%{label}: %{value} Paxs'  # Hover mostra o valor e porcentagem
    )

    # Ajustar o layout para garantir que o texto caiba dentro da pizza
    fig.update_layout(
        showlegend=True,  # Legenda para facilitar a visualização
        margin=dict(t=50, b=50, l=50, r=50)  # Ajuste de margens se necessário
    )

    return fig

def gerar_df_vendedor_filtrado(df_cont_passeio, passeios_incluidos):

    df_vendedor_filtrado = df_cont_passeio[(df_cont_passeio['Vendedor'] == vendedor) & (df_cont_passeio['Nome_Servico'].isin(passeios_incluidos))]

    soma_catamara = df_vendedor_filtrado.loc[df_vendedor_filtrado['Nome_Servico'].isin(['CATAMARÃ DO FORRÓ', 'EMBARCAÇAO - CATAMARÃ DO FORRÓ ']), 'Total Paxs'].sum()

    soma_bynight = df_vendedor_filtrado.loc[df_vendedor_filtrado['Nome_Servico'].isin(['BY NIGHT PARAHYBA OXENTE ', 'INGRESSO - BY NIGHT ']), 'Total Paxs'].sum()

    df_vendedor_filtrado.loc[df_vendedor_filtrado['Nome_Servico'] == 'BY NIGHT PARAHYBA OXENTE ', 'Total Paxs'] = soma_bynight

    df_vendedor_filtrado.loc[df_vendedor_filtrado['Nome_Servico'] == 'CATAMARÃ DO FORRÓ', 'Total Paxs'] = soma_catamara

    return df_vendedor_filtrado

def gerar_grafico_pizza_vendedor(df_vendedor_filtrado, vendedor, passeios_incluidos):

    fig = px.pie(df_vendedor_filtrado, names='Nome_Servico', 
        values='Total Paxs', 
        title=f'Distribuição de Paxs por Passeio - {vendedor}',
        category_orders={'Nome_Servico': passeios_incluidos},
    )
    fig.update_traces(
        texttemplate='%{percent}',  # Apenas a porcentagem dentro da fatia
        hovertemplate='%{label}: %{value} Paxs'  # Hover mostra o valor e porcentagem
    )
    
    # Ajustar o layout para garantir que o texto caiba dentro da pizza
    fig.update_layout(
        showlegend=True,  # Legenda para facilitar a visualização
        margin=dict(t=50, b=50, l=50, r=50)  # Ajuste de margens se necessário
    )

    return fig

def adicionar_historico_de_vendas(df_historico, df_vendas):

    df_historico = df_historico.rename(columns={'Data': 'Data_Venda', 'Paxs ADT': 'Total_Paxs'})

    df_vendas = pd.concat([df_vendas, df_historico[['Data_Venda', 'Setor', 'Valor_Venda', 'Total_Paxs', 'Mes_Ano']]], ignore_index=True)

    return df_vendas, df_historico

def gerar_df_vendas_group(df_vendas, df_reembolsos):

    df_vendas_group = df_vendas.groupby(['Vendedor', 'Setor', 'Mes_Ano'], dropna=False, as_index=False).agg({'Valor_Venda': 'sum','Total_Paxs': 'mean','Desconto_Global_Ajustado': 'sum'})

    df_reembolsos = df_reembolsos.groupby('Vendedor', as_index=False)['Valor_Total'].sum()

    df_merged = pd.merge(df_vendas_group, df_reembolsos, on='Vendedor', how='left')

    df_vendas_group['Venda_Filtrada'] = df_merged['Valor_Venda'] - df_merged['Valor_Total'].fillna(0)

    df_vendas_group = df_vendas_group.sort_values(by='Venda_Filtrada', ascending=False)

    return df_vendas_group, df_reembolsos

def gerar_df_vendas_grouop_setor(df_vendas_group):

    df_vendas_group_setor = df_vendas_group.groupby('Setor', as_index=False).agg({'Venda_Filtrada': 'sum','Total_Paxs': 'mean'})

    df_vendas_group_setor = df_vendas_group_setor.sort_values(by='Venda_Filtrada', ascending=False)

    df_vendas_group_setor = df_vendas_group_setor[df_vendas_group_setor['Setor'].isin(st.session_state.setores_desejados_gerencial)]

    df_vendas_group_setor['Ticket_Medio'] = df_vendas_group_setor['Venda_Filtrada'] / df_vendas_group_setor['Total_Paxs']

    return df_vendas_group_setor

def gerar_grafico_valor_total_setor(df_vendas_group_setor):

    fig = px.bar(
            x=df_vendas_group_setor['Setor'], 
            y=df_vendas_group_setor['Venda_Filtrada'],
            color=df_vendas_group_setor['Setor'],  # Adiciona cores diferentes para cada setor
            title='Valor Total por Setor',
            labels={'Venda_Filtrada': 'Valor Total', 'Setor': 'Setores'},
            text=df_vendas_group_setor['Venda_Filtrada'].apply(formatar_moeda),
            color_discrete_sequence=['#047c6c']  
    )

    ticket_medio_line = px.line(
        x=df_vendas_group_setor['Setor'],
        y=df_vendas_group_setor['Ticket_Medio'],
        line_shape='spline'
    )
    fig.add_trace(ticket_medio_line.data[0])
    fig.data[-1].name = 'Ticket Medio'
    fig.data[-1].line.color = 'orange'
    fig.data[-1].line.width = 1  # Diminuindo a espessura do spline
    fig.data[-1].yaxis = 'y2'
    fig.data[-1].mode = 'lines+markers+text'
    fig.data[-1].marker = dict(size=8, color='orange')
    fig.data[-1].text = df_vendas_group_setor['Ticket_Medio'].apply(formatar_moeda)
    fig.data[-1].textfont = dict(color='orange')  # Definindo a cor do texto dos marcadores


    fig.data[-1].textposition = 'top center'

    fig.update_traces(
        textposition='outside',
        textfont=dict(size=10, color='green'),
        selector=dict(type='bar')
    )
    fig.update_layout(
        yaxis_title='Valor Total',
        xaxis_title='Setores',
        yaxis2=dict(title='Ticket Medio', overlaying='y', side='right', showgrid=False, zeroline=False, range=[-500, 400]  )
    )

    return fig

def gerar_df_vendas_group_mes_setor(df_vendas, df_reembolsos):

    df_vendas_group_mes = df_vendas.groupby(['Mes_Ano', 'Vendedor', 'Setor'], dropna=False).agg({'Valor_Venda': 'sum', 'Total_Paxs': 'mean', 'Desconto_Global_Ajustado': 'sum'}).reset_index()

    df_merged = pd.merge(df_vendas_group_mes, df_reembolsos, on='Vendedor', how='left')

    df_vendas_group_mes['Venda_Filtrada'] = df_merged['Valor_Venda'] - df_merged['Valor_Total'].fillna(0)

    df_vendas_group_mes['Mes_Ano'] = df_vendas_group_mes['Mes_Ano'].dt.to_timestamp()

    df_vendas_group_mes_setor = df_vendas_group_mes.groupby(['Mes_Ano', 'Setor'], as_index=False).agg({'Venda_Filtrada': 'sum'})

    df_vendas_group_mes_setor['Mes_Ano'] = df_vendas_group_mes_setor['Mes_Ano'].dt.strftime('%B %Y')

    return df_vendas_group_mes_setor

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

    soma_catamara = df_ranking.loc[df_ranking['Servico'].isin(['CATAMARÃ DO FORRÓ', 'EMBARCAÇAO - CATAMARÃ DO FORRÓ ']), 'Total Paxs'].sum()

    soma_bynight = df_ranking.loc[df_ranking['Servico'].isin(['BY NIGHT PARAHYBA OXENTE ', 'INGRESSO - BY NIGHT ']), 'Total Paxs'].sum()

    ranking_filtrado_combo = df_ranking.groupby(['Setor', 'Servico', 'Mes_Ano'], as_index=False)['Total Paxs'].sum()

    ranking_filtrado_combo_setores = ranking_filtrado_combo[ranking_filtrado_combo['Setor'].isin(st.session_state.setores_desejados_gerencial)]

    ranking_filtrado = df_ranking[df_ranking['Servico'].isin(passeios_incluidos)]

    ranking_filtrado = ranking_filtrado.groupby(['Setor', 'Servico', 'Mes_Ano'], as_index=False)['Total Paxs'].sum()

    ranking_filtrado_setores = ranking_filtrado[ranking_filtrado['Setor'].isin(st.session_state.setores_desejados_gerencial)]

    ranking_filtrado_geral = ranking_filtrado.groupby(['Servico', 'Mes_Ano'], as_index=False)['Total Paxs'].sum()

    mes_ranking_geral = ranking_filtrado_geral['Mes_Ano'].dt.strftime('%B %Y').unique()

    ranking_filtrado_geral.loc[ranking_filtrado_geral['Servico'] == 'BY NIGHT PARAHYBA OXENTE ', 'Total Paxs'] = soma_bynight

    ranking_filtrado_geral.loc[ranking_filtrado_geral['Servico'] == 'CATAMARÃ DO FORRÓ', 'Total Paxs'] = soma_catamara

    return ranking_filtrado_combo_setores, ranking_filtrado_setores, ranking_filtrado_geral, mes_ranking_geral

def plotar_graficos_pizza_desempenho_passeios_geral(mes_ranking_geral, ranking_filtrado_geral, colunas):

    i = 0

    for mes_geral in mes_ranking_geral:

        df_ranking_geral_chart = ranking_filtrado_geral[(ranking_filtrado_geral['Mes_Ano'].dt.strftime('%B %Y') == mes_geral)]

        if not df_ranking_geral_chart.empty:

            fig_1 = px.pie(
                df_ranking_geral_chart,
                names='Servico',
                values='Total Paxs',
                title=f'Desempenho Passeios Geral - {mes_geral}',
                labels={'Total Paxs': 'Total Paxs', 'Passeio': 'Servico'},
                hole=0.3,  # Para criar um gráfico de donut
                category_orders={'Servico': sorted(df_ranking_geral_chart['Servico'].unique())}
                )
            
            colunas[i % 2].plotly_chart(fig_1)

            i+=1

def plotar_graficos_pizza_desempenho_passeios_por_setor(ranking_filtrado_setores, ranking_filtrado_combo_setores):

    mes_ranking = ranking_filtrado_setores['Mes_Ano'].dt.strftime('%B %Y').unique()

    setor_ranking = ranking_filtrado_setores['Setor'].unique()

    todos_servicos = ranking_filtrado_combo_setores['Servico'].unique()
    
    combo_outros = [combo for combo in todos_servicos if combo not in st.session_state.combo_luck]

    for mes_ in mes_ranking:

        for setor_ in setor_ranking:

            df_ranking_chart = ranking_filtrado_setores[(ranking_filtrado_setores['Mes_Ano'].dt.strftime('%B %Y') == mes_) & (ranking_filtrado_setores['Setor'] == setor_)]
            
            df_ranking_combos = ranking_filtrado_combo_setores[(ranking_filtrado_combo_setores['Mes_Ano'].dt.strftime('%B %Y') == mes_) & (ranking_filtrado_combo_setores['Setor'] == setor_)]

            df_ranking_combos['Combo'] = df_ranking_combos['Servico'].apply(lambda x: 'MIX LUCK' if x.upper() in st.session_state.combo_luck else 'MIX OUTROS' if x.upper() in combo_outros else 
                                                                            'OUTROS')

            df_combos_contador = df_ranking_combos.groupby('Combo')['Total Paxs'].sum().reset_index()
            
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

        st.session_state.id_gsheet_reembolsos = '1dmcVUq7Bl_ipxPyxY8IcgxT7dCmTh_FLxYJqGigoSb0'

        st.session_state.lista_colunas_numero_df_reembolsos = ['Valor_Total']

        st.session_state.lista_colunas_data_df_reembolsos = ['Data_venc']

        st.session_state.meses_disponiveis = {'Janeiro': 1, 'Fevereiro': 2, 'Março': 3, 'Abril': 4, 'Maio': 5, 'Junho': 6, 'Julho': 7, 'Agosto': 8, 'Setembro': 9, 'Outubro': 10, 'Novembro': 11, 
                                              'Dezembro': 12}
        
        st.session_state.setores_desejados_gerencial = ['EVENTOS', 'GRUPOS', 'GUIA', 'HOTEL VENDAS', 'PDV', 'VENDAS ONLINE']

        st.session_state.combo_luck = ['CATAMARÃ DO FORRÓ', 'CITY TOUR', 'EMBARCAÇAO - CATAMARÃ DO FORRÓ ',  'EMBARCAÇÃO - ILHA DE AREIA VERMELHA', 'EMBARCAÇÃO - PASSEIO PELO RIO PARAÍBA', 
                                       'ILHA DE AREIA VERMELHA', 'EMBARCAÇÃO - PISCINAS DO EXTREMO ORIENTAL', 'ENTARDECER NA PRAIA DO JACARÉ ', 'LITORAL NORTE COM ENTARDECER NA PRAIA DO JACARÉ', 
                                       'PISCINAS DO EXTREMO ORIENTAL', 'PRAIAS DA COSTA DO CONDE']

st.title('Gerencial - Mês a Mês')

st.divider()

if not 'df_vendas' in st.session_state:

    with st.spinner('Puxando vendas manuais, reembolsos, metas de vendedores, metas de setores, configurações, histórico...'):

        gerar_df_vendas_manuais()

        gerar_df_reembolsos()

        gerar_df_metas()

        puxar_aba_simples(st.session_state.id_gsheet_metas_vendas, 'Configurações Vendas', 'df_config')

        gerar_df_historico()

    with st.spinner('Puxando vendas, ranking, guias IN e paxs IN do Phoenix...'):

        gerar_df_vendas()

        st.session_state.df_vendas_final = gerar_df_vendas_final()

        st.session_state.anos_disponiveis = st.session_state.df_vendas_final['Ano'].unique().tolist()

        gerar_df_ranking()

        gerar_df_paxs_in()

locale.setlocale(locale.LC_TIME, 'pt_BR.UTF-8')

col1, col2 = st.columns([2, 4])

with col1:

    ano_selecao = st.multiselect('Selecione o Ano:', st.session_state.anos_disponiveis, default=[], key='ano_selecao')

with col2:

    mes_selecao = st.multiselect('Selecione o Mês:', st.session_state.meses_disponiveis.keys(), default=st.session_state.meses_disponiveis.keys(), key='mes_selecao')

    st.session_state.mes_selecao_valores = [st.session_state.meses_disponiveis[mes] for mes in mes_selecao]

if len(ano_selecao)>0 and len(mes_selecao)>0:

    df_vendas, df_paxs_in, df_reembolsos, df_historico, df_ranking = filtrar_periodo_dfs()

    df_vendas = adicionar_total_paxs_periodo_vendas(df_paxs_in, df_vendas)

    df_vendas = ajuste_colunas_float(df_vendas)

    df_vendas, df_historico = adicionar_historico_de_vendas(df_historico, df_vendas)

    df_vendas = ajustar_desconto_global(df_vendas)

    df_vendas_group, df_reembolsos = gerar_df_vendas_group(df_vendas, df_reembolsos)

    df_vendas_group_setor = gerar_df_vendas_grouop_setor(df_vendas_group)

    fig = gerar_grafico_valor_total_setor(df_vendas_group_setor)

    st.plotly_chart(fig)

    df_vendas_group_mes_setor = gerar_df_vendas_group_mes_setor(df_vendas, df_reembolsos)

    st.title('Fatias de Vendas por Setor')

    colunas = st.columns(2)

    plotar_graficos_pizza_vendas_setor_mes(df_vendas_group_mes_setor, colunas)

    st.title('Desempenho Passeios Geral')

    passeios_incluidos = st.session_state.df_config[st.session_state.df_config['Configuração']=='Passeios Gráfico Pizza']['Parâmetro'].tolist()

    ranking_filtrado_combo_setores, ranking_filtrado_setores, ranking_filtrado_geral, mes_ranking_geral = gerar_rankings_filtrados_geral(df_ranking, passeios_incluidos)

    colunas = st.columns(2)

    plotar_graficos_pizza_desempenho_passeios_geral(mes_ranking_geral, ranking_filtrado_geral, colunas)

    st.title('Desempenho Passeios Por Setor')

    plotar_graficos_pizza_desempenho_passeios_por_setor(ranking_filtrado_setores, ranking_filtrado_combo_setores)

else:

    st.warning('Seleciona pelo menos um ano e um mês para a análise')