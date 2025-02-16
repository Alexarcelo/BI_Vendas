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

def gerar_df_metas():

    puxar_aba_simples(st.session_state.id_gsheet_metas_vendas, 'BD - Metas', 'df_metas')

    tratar_colunas_numero_df(st.session_state.df_metas, st.session_state.lista_colunas_numero_df_metas)

    tratar_colunas_data_df(st.session_state.df_metas, st.session_state.lista_colunas_data_df_metas)

    st.session_state.df_metas['Mes_Ano'] = pd.to_datetime(st.session_state.df_metas['Data']).dt.to_period('M')

    st.session_state.df_metas['Meta_Total'] = st.session_state.df_metas['Meta_Guia'] + st.session_state.df_metas['Meta_PDV'] + st.session_state.df_metas['Meta_HV'] + \
        st.session_state.df_metas['Meta_Grupos'] + st.session_state.df_metas['Meta_VendasOnline']

def gerar_df_historico():

    puxar_aba_simples(st.session_state.id_gsheet_metas_vendas, 'BD - Historico', 'df_historico')

    tratar_colunas_numero_df(st.session_state.df_historico, st.session_state.lista_colunas_numero_df_historico)

    tratar_colunas_data_df(st.session_state.df_historico, st.session_state.lista_colunas_data_df_historico)

    st.session_state.df_historico['Ano'] = pd.to_datetime(st.session_state.df_historico['Data']).dt.year
    
    st.session_state.df_historico['Mes'] = pd.to_datetime(st.session_state.df_historico['Data']).dt.month
    
    st.session_state.df_historico['Mes_Ano'] = pd.to_datetime(st.session_state.df_historico['Data']).dt.to_period('M')

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

def gerar_df_ranking():

    request_select = '''SELECT `1 Vendedor`, `Data de Execucao`, `Tipo de Servico`, `Servico`, `Total ADT`, `Total CHD`, `Codigo da Reserva` 
    FROM vw_sales_ranking 
    WHERE `Tipo de Servico` = 'TOUR';'''
    
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

def adicionar_historico_de_vendas(df_historico, df_vendas):

    df_historico = df_historico.rename(columns={'Data': 'Data_Venda', 'Paxs ADT': 'Total_Paxs'})

    df_vendas = pd.concat([df_vendas, df_historico[['Data_Venda', 'Setor', 'Valor_Venda', 'Total_Paxs', 'Mes_Ano']]], ignore_index=True)

    return df_vendas, df_historico

def ajustar_desconto_global(df_vendas):

    df_vendas['Desconto_Global_Ajustado'] = np.where((df_vendas['Desconto_Global_Por_Servico'].notna()) & (df_vendas['Desconto_Global_Por_Servico'] < 1000) & 
                                                     (df_vendas['Nome_Servico'] != 'EXTRA'), df_vendas['Desconto_Global_Por_Servico'], 0)
    
    return df_vendas

def gerar_df_vendas_agrupado(df_vendas, df_reembolsos, df_metas_setor, df_paxs_in):

    def calculando_soma_total_paxs_paxs_desc(df_paxs_in, df_metas_setor, df_vendas_agrupado):

        total_paxs_in = df_paxs_in['Total_Paxs'].sum()

        total_paxs_desc = df_metas_setor['Paxs_Desc'].sum()

        df_vendas_agrupado['Total_Paxs'] = total_paxs_in + total_paxs_desc

        return df_vendas_agrupado
    
    def adicionar_reembolsos(df_reembolsos, df_vendas_agrupado):

        df_reembolsos = df_reembolsos.groupby('Vendedor', as_index=False)['Valor_Total'].sum()

        df_vendas_agrupado = pd.merge(df_vendas_agrupado, df_reembolsos, on='Vendedor', how='left')

        return df_vendas_agrupado
    
    def calculando_ordenando_venda_liquida_reembolsos(df_vendas_agrupado):

        df_vendas_agrupado['Venda_Filtrada'] = df_vendas_agrupado['Valor_Venda'] - df_vendas_agrupado['Valor_Total'].fillna(0)

        df_vendas_agrupado = df_vendas_agrupado.sort_values(by='Venda_Filtrada', ascending=False)

        return df_vendas_agrupado

    df_vendas_agrupado = df_vendas.groupby(['Vendedor', 'Setor', 'Mes_Ano'], dropna=False, as_index=False).agg({'Valor_Venda': 'sum', 'Desconto_Global_Ajustado': 'sum'})

    df_vendas_agrupado = calculando_soma_total_paxs_paxs_desc(df_paxs_in, df_metas_setor, df_vendas_agrupado)

    df_vendas_agrupado = adicionar_reembolsos(df_reembolsos, df_vendas_agrupado)

    df_vendas_agrupado = calculando_ordenando_venda_liquida_reembolsos(df_vendas_agrupado)

    return df_vendas_agrupado, df_reembolsos

def gerar_df_vendas_agrupado_setor(df_vendas_agrupado):

    df_vendas_agrupado_setor = df_vendas_agrupado.groupby('Setor', as_index=False).agg({'Venda_Filtrada': 'sum','Total_Paxs': 'mean'})

    df_vendas_agrupado_setor = df_vendas_agrupado_setor.sort_values(by='Venda_Filtrada', ascending=False)

    df_vendas_agrupado_setor = df_vendas_agrupado_setor[df_vendas_agrupado_setor['Setor'].isin(st.session_state.setores_desejados_gerencial)]

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

def gerar_df_vendas_agrupado_mes_setor(df_vendas, df_metas_setor, df_reembolsos):

    def calculando_soma_total_paxs_paxs_desc(df_paxs_in, df_metas_setor, df_vendas_agrupado_mes):

        total_paxs_in = df_paxs_in['Total_Paxs'].sum()

        total_paxs_desc = df_metas_setor['Paxs_Desc'].sum()

        df_vendas_agrupado_mes['Total_Paxs'] = total_paxs_in + total_paxs_desc

        return df_vendas_agrupado_mes

    df_vendas_agrupado_mes = df_vendas.groupby(['Mes_Ano', 'Vendedor', 'Setor'], dropna=False).agg({'Valor_Venda': 'sum', 'Desconto_Global_Ajustado': 'sum'}).reset_index()

    df_vendas_agrupado_mes = calculando_soma_total_paxs_paxs_desc(df_paxs_in, df_metas_setor, df_vendas_agrupado_mes)

    df_vendas_agrupado_mes = pd.merge(df_vendas_agrupado_mes, df_reembolsos, on='Vendedor', how='left')

    df_vendas_agrupado_mes['Venda_Filtrada'] = df_vendas_agrupado_mes['Valor_Venda'].fillna(0) - df_vendas_agrupado_mes['Valor_Total'].fillna(0)

    df_vendas_agrupado_mes_setor = df_vendas_agrupado_mes.groupby(['Mes_Ano', 'Setor'], as_index=False).agg({'Venda_Filtrada': 'sum'})

    df_vendas_agrupado_mes_setor['Mes_Ano'] = df_vendas_agrupado_mes_setor['Mes_Ano'].dt.strftime('%B %Y')

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

        ranking_filtrado_combo_setores = ranking_filtrado_combo[ranking_filtrado_combo['Setor'].isin(st.session_state.setores_desejados_gerencial)]

        return ranking_filtrado_combo_setores
    
    def gerar_ranking_filtrado(df_ranking):

        ranking_filtrado = df_ranking[df_ranking['Servico'].isin(passeios_incluidos)]

        ranking_filtrado = ranking_filtrado.groupby(['Setor', 'Servico', 'Mes_Ano'], as_index=False)['Total Paxs'].sum()

        return ranking_filtrado

    df_ranking['Servico'] = df_ranking['Servico'].replace({'EMBARCAÇAO - CATAMARÃ DO FORRÓ ': 'CATAMARÃ DO FORRÓ', 'INGRESSO - BY NIGHT ': 'BY NIGHT PARAHYBA OXENTE '}) 

    ranking_filtrado_combo_setores = gerar_ranking_filtrado_combo_setores(df_ranking)

    ranking_filtrado = gerar_ranking_filtrado(df_ranking)

    ranking_filtrado_setores = ranking_filtrado[ranking_filtrado['Setor'].isin(st.session_state.setores_desejados_gerencial)]

    ranking_filtrado_geral = ranking_filtrado.groupby(['Servico', 'Mes_Ano'], as_index=False)['Total Paxs'].sum()

    mes_ranking_geral = ranking_filtrado_geral['Mes_Ano'].dt.strftime('%B %Y').unique()

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
                hole=0.3,
                category_orders={'Servico': sorted(df_ranking_geral_chart['Servico'].unique())}
                )
            
            colunas[i % 2].plotly_chart(fig_1)

            i+=1

def plotar_graficos_pizza_desempenho_passeios_por_setor(ranking_filtrado_setores, ranking_filtrado_combo_setores):

    mes_ranking = ranking_filtrado_setores['Mes_Ano'].dt.strftime('%B %Y').unique()

    setor_ranking = ranking_filtrado_setores['Setor'].unique()

    for mes_ in mes_ranking:

        for setor_ in setor_ranking:

            df_ranking_chart = ranking_filtrado_setores[(ranking_filtrado_setores['Mes_Ano'].dt.strftime('%B %Y') == mes_) & (ranking_filtrado_setores['Setor'] == setor_)]
            
            df_ranking_combos = ranking_filtrado_combo_setores[(ranking_filtrado_combo_setores['Mes_Ano'].dt.strftime('%B %Y') == mes_) & (ranking_filtrado_combo_setores['Setor'] == setor_)]

            df_ranking_combos['Combo'] = df_ranking_combos['Servico'].apply(lambda x: 'MIX LUCK' if x.upper() in st.session_state.combo_luck else 'MIX OUTROS')

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

if any(key not in st.session_state for key in ['df_reembolsos', 'df_metas', 'df_config', 'df_historico', 'df_vendas_final', 'anos_disponiveis', 'df_ranking', 'df_paxs_in']):

    with st.spinner('Puxando reembolsos, metas de vendedores, metas de setores, configurações, histórico...'):

        gerar_df_reembolsos()

        gerar_df_metas()

        puxar_aba_simples(st.session_state.id_gsheet_metas_vendas, 'Configurações Vendas', 'df_config')

        st.session_state.passeios_incluidos = st.session_state.df_config[st.session_state.df_config['Configuração']=='Passeios Gráfico Pizza']['Parâmetro'].tolist()

        gerar_df_historico()

    with st.spinner('Puxando vendas, ranking, guias IN e paxs IN do Phoenix...'):

        st.session_state.df_vendas_final = gerar_df_vendas_final()

        st.session_state.anos_disponiveis = st.session_state.df_vendas_final['Ano'].unique().tolist()

        gerar_df_ranking()

        gerar_df_paxs_in()

locale.setlocale(locale.LC_TIME, 'pt_BR.UTF-8')

col1, col2 = st.columns([2, 4])

with col1:

    ano_selecao = st.multiselect('Selecione o Ano:', st.session_state.anos_disponiveis, default=[date.today().year], key='ano_selecao')

with col2:

    mes_selecao = st.multiselect('Selecione o Mês:', st.session_state.meses_disponiveis.keys(), default=list(st.session_state.meses_disponiveis.keys())[:date.today().month], key='mes_selecao')

    st.session_state.mes_selecao_valores = [st.session_state.meses_disponiveis[mes] for mes in mes_selecao]

if len(ano_selecao)>0 and len(mes_selecao)>0:

    df_vendas, df_paxs_in, df_reembolsos, df_historico, df_ranking = filtrar_periodo_dfs()

    df_vendas, df_historico = adicionar_historico_de_vendas(df_historico, df_vendas)

    df_vendas = ajustar_desconto_global(df_vendas)

    df_vendas_agrupado, df_reembolsos = gerar_df_vendas_agrupado(df_vendas, df_reembolsos, st.session_state.df_metas, df_paxs_in)

    df_vendas_agrupado_setor = gerar_df_vendas_agrupado_setor(df_vendas_agrupado)

    fig = gerar_grafico_valor_total_setor(df_vendas_agrupado_setor)

    st.plotly_chart(fig)

    df_vendas_agrupado_mes_setor = gerar_df_vendas_agrupado_mes_setor(df_vendas, st.session_state.df_metas, df_reembolsos)

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
