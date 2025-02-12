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

def gerar_df_guias_in():

    request_select = '''
        SELECT 
        `Data da Escala`,
        `Guia`,
        `Tipo de Servico`,
        `Total ADT`,
        `Total CHD` 
        FROM vw_payment_guide
        WHERE `Tipo de Servico` = 'IN';
        '''
    
    st.session_state.df_guias_in = gerar_df_phoenix(st.session_state.base_luck, request_select)

    st.session_state.df_guias_in['Total_Paxs'] = st.session_state.df_guias_in['Total ADT'] + (st.session_state.df_guias_in['Total CHD'] / 2)

    st.session_state.df_guias_in['Data da Escala'] = pd.to_datetime(st.session_state.df_guias_in['Data da Escala']).dt.date

    substituicao = {'RAQUEL - PDV': 'RAQUEL - GUIA', 'VALERIA - PDV': 'VALERIA - GUIA', 'ROBERTA - PDV': 'ROBERTA - GUIA', 'LETICIA - TRANSFERISTA': 'LETICIA - GUIA', 
                    'SORAYA - BASE AEROPORTO ': 'SORAYA - GUIA', 'SORAYA - TRANSFERISTA': 'SORAYA - GUIA'}

    st.session_state.df_guias_in['Guia'] = st.session_state.df_guias_in['Guia'].replace(substituicao)

def gerar_df_metas_vendedor():

    puxar_aba_simples(st.session_state.id_gsheet_metas_vendas, 'BD - Metas_Vendedor', 'df_metas_vendedor')

    tratar_colunas_numero_df(st.session_state.df_metas_vendedor, st.session_state.lista_colunas_numero_df_metas_vendedor)

    tratar_colunas_data_df(st.session_state.df_metas_vendedor, st.session_state.lista_colunas_data_df_metas_vendedor)

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
    
    st.session_state.df_paxs_in['Mes_Ano'] = pd.to_datetime(st.session_state.df_paxs_in['Data Execucao']).dt.to_period('M')

    st.session_state.df_paxs_in['Total_Paxs_Periodo'] = st.session_state.df_paxs_in['Total ADT'] + (st.session_state.df_paxs_in['Total CHD'] / 2)

    st.session_state.df_paxs_in = pd.merge(st.session_state.df_paxs_in, st.session_state.df_metas[['Mes_Ano', 'Paxs_Desc']], on='Mes_Ano', how='left')

    st.session_state.df_paxs_in['Paxs_Desc'] = pd.to_numeric(st.session_state.df_paxs_in['Paxs_Desc'], errors='coerce')

def gerar_df_metas():

    puxar_aba_simples(st.session_state.id_gsheet_metas_vendas, 'BD - Metas', 'df_metas')

    tratar_colunas_numero_df(st.session_state.df_metas, st.session_state.lista_colunas_numero_df_metas)

    tratar_colunas_data_df(st.session_state.df_metas, st.session_state.lista_colunas_data_df_metas)

    st.session_state.df_metas['Mes_Ano'] = pd.to_datetime(st.session_state.df_metas['Data']).dt.to_period('M')

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

def adicionar_colunas_paxs_in_total_paxs_ajuste_colunas_float(df_vendas, df_guias_in):

    df_vendas = df_vendas.merge(df_guias_in[['Guia', 'Total_Paxs']], left_on='Vendedor', right_on='Guia', how='left')

    df_vendas.rename(columns={'Total_Paxs_y': 'Paxs_IN', 'Total_Paxs_x': 'Total_Paxs'}, inplace=True)

    for coluna in ['Valor_Venda', 'Total_Paxs', 'Meta']:

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

    valor_ref = np.where(df_vendas['Data_Venda'] >= datetime.date(2024, 12, 1), 1000, 5000)

    df_vendas['Desconto_Global_Ajustado'] = np.where((df_vendas['Desconto_Global_Por_Servico'].notna()) & (df_vendas['Desconto_Global_Por_Servico'] < valor_ref) & 
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

        st.session_state.id_gsheet_reembolsos = '1dmcVUq7Bl_ipxPyxY8IcgxT7dCmTh_FLxYJqGigoSb0'

        st.session_state.lista_colunas_numero_df_reembolsos = ['Valor_Total']

        st.session_state.lista_colunas_data_df_reembolsos = ['Data_venc']

st.title('Vendas Gerais')

st.divider()

if not 'df_vendas' in st.session_state:

    with st.spinner('Puxando vendas manuais, reembolsos, metas de vendedores, metas de setores...'):

        gerar_df_vendas_manuais()

        gerar_df_reembolsos()

        gerar_df_metas_vendedor()

        gerar_df_metas()

    with st.spinner('Puxando vendas, ranking, guias IN e paxs IN do Phoenix...'):

        gerar_df_vendas()

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

    seleciona_setor = st.multiselect('Setor', sorted(lista_setor), default=None)

    if len(seleciona_setor)>0:

        df_vendas, df_paxs_in, df_metas_vendedor, df_reembolsos, df_guias_in, df_metas_setor = filtrar_periodo_dfs()

        df_vendas = adicionar_total_paxs_periodo_vendas(df_paxs_in, df_vendas)

        df_reembolsos = df_reembolsos.groupby('Vendedor', as_index=False)['Valor_Total'].sum()

        df_guias_in = df_guias_in.groupby('Guia', as_index=False)['Total_Paxs'].sum()

        if not '--- Todos ---' in seleciona_setor:

            df_vendas = df_vendas[df_vendas['Setor'].isin(seleciona_setor)]

        lista_vendedor, lista_canal, lista_hotel = criar_listas_vendedor_canal_hotel(df_vendas)

        if seleciona_setor[0]=='VENDAS ONLINE':

            seleciona_canal = st.multiselect('Canal de Vendas', lista_canal, key='Can_on')

        else:

            seleciona_canal = []

        seleciona_vend = st.multiselect('Vendedor', lista_vendedor, key='Ven_on')

        if seleciona_setor[0]=='HOTEL VENDAS':

            seleciona_hotel = st.multiselect('Hotel', lista_hotel, key='Hot_on')

        else:

            seleciona_hotel = []

        df_vendas = filtrar_canal_vendedor_hotel_df_vendas(df_vendas, seleciona_canal, seleciona_vend, seleciona_hotel)

        df_hotel = gerar_df_hotel(df_vendas)

        df_vendas = adicionar_colunas_paxs_in_total_paxs_ajuste_colunas_float(df_vendas, df_guias_in)

        df_contador = gerar_df_contador(df_vendas)

        df_vendas = ajustar_desconto_global(df_vendas)

        df_vendas_agrupado = gerar_df_vendas_agrupado(df_vendas, df_reembolsos, df_contador, df_metas_vendedor)

        df_cont_passeio = df_vendas.groupby(['Vendedor', 'Nome_Servico'], as_index=False)['Total Paxs'].sum()

        with col2:
                
            col2_1, col2_2 = st.columns([2,5])

            with col2_1:

                with st.container():

                    soma_vendas, tm_vendas, tm_setor_estip, total_desconto, paxs_recebidos, med_desconto = gerar_soma_vendas_tm_vendas_desconto_paxs_recebidos(df_vendas_agrupado, df_vendas)

                    if len(seleciona_setor)==1 and seleciona_setor[0]=='--- Todos ---':

                        meta_esperada_formatada, perc_alcancado = gerar_meta_esperada_perc_alcancado_todos_setores(df_vendas_agrupado, soma_vendas)

                    else:

                        meta_esperada_formatada, perc_alcancado = gerar_meta_esperada_perc_alcancado_setor_especifico(df_vendas_agrupado, soma_vendas)

                    plotar_quadrados_html('Valor Total Vendido', formatar_moeda(soma_vendas))

                    plotar_quadrados_html('Meta Estimada', meta_esperada_formatada)

                    plotar_quadrados_html('Meta de TM', tm_setor_estip)

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

                df_setor_agrupado = df_vendas_agrupado.groupby('Setor', as_index=False)['Venda_Filtrada'].sum()

                if not df_setor_agrupado.empty:

                    fig = gerar_grafico_todos_setores(df_setor_agrupado)

            else:
                
                if not df_vendas_agrupado.empty:

                    fig = gerar_grafico_setor_especifico(df_vendas_agrupado)

                else:

                    fig = gerar_grafico_sem_dados()

            st.plotly_chart(fig, key="key_1")

row0 = st.columns(2)

passeios_incluidos = ['PRAIAS DA COSTA DO CONDE', 'ILHA DE AREIA VERMELHA', 'CITY TOUR', 'LITORAL NORTE COM ENTARDECER NA PRAIA DO JACARÉ', 'EMBARCAÇÃO - PASSEIO PELO RIO PARAÍBA', 
                      'PISCINAS DO EXTREMO ORIENTAL', 'PRAIA BELA', 'CATAMARÃ DO FORRÓ', 'BY NIGHT PARAHYBA OXENTE ', 'INGRESSO - BY NIGHT ', 'EMBARCAÇAO - CATAMARÃ DO FORRÓ ', 
                      'TRILHA DOS MIRANTES DA COSTA DO CONDE', 'TRILHA DOS COQUEIRAIS']

if len(seleciona_setor)==1 and seleciona_setor[0] == '--- Todos ---':

    df_todos_vendedores_filtrado = gerar_df_todos_vendedores_filtrado(df_cont_passeio, passeios_incluidos)

    if not df_todos_vendedores_filtrado.empty:

        fig = gerar_grafico_pizza_todos_vendedores(df_todos_vendedores_filtrado, passeios_incluidos)

        st.plotly_chart(fig)

elif len(seleciona_setor)>0:

    coluna = 0

    for vendedor in df_cont_passeio['Vendedor'].unique():

        df_vendedor_filtrado = gerar_df_vendedor_filtrado(df_cont_passeio, passeios_incluidos)
        
        if not df_vendedor_filtrado.empty:

            fig = gerar_grafico_pizza_vendedor(df_vendedor_filtrado, vendedor, passeios_incluidos)

            with row0[coluna]:
        
                st.plotly_chart(fig)

            if coluna==0:

                coluna = 1

            else:

                coluna = 0
