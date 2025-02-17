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

def gerar_df_metas():

    puxar_aba_simples(st.session_state.id_gsheet_metas_vendas, 'BD - Metas', 'df_metas')

    tratar_colunas_numero_df(st.session_state.df_metas, st.session_state.lista_colunas_numero_df_metas)

    tratar_colunas_data_df(st.session_state.df_metas, st.session_state.lista_colunas_data_df_metas)

    st.session_state.df_metas['Mes_Ano'] = pd.to_datetime(st.session_state.df_metas['Data']).dt.to_period('M')

    st.session_state.df_metas['Meta_Total'] = st.session_state.df_metas['Meta_Guia'] + st.session_state.df_metas['Meta_PDV'] + st.session_state.df_metas['Meta_HV'] + \
        st.session_state.df_metas['Meta_Grupos'] + st.session_state.df_metas['Meta_VendasOnline']

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

def gerar_df_paxs_in():

    request_select = '''SELECT Reserva, Parceiro, `Tipo de Servico`, `Data Execucao`, `Servico`, `Status do Servico`, `Total ADT`, `Total CHD` 
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

def criar_df_vendas_agrupado():

    df_vendas = st.session_state.df_vendas_final

    df_vendas = df_vendas[(df_vendas['Setor'] != 'LOGISTICA') & (~df_vendas['Nome_Estabelecimento_Origem'].isin(['SEM HOTEL ', 'AEROPORTO JOÃO PESSOA'])) & (df_vendas['Vendedor'] != 'ComeiaLabs')]

    df_vendas['Mes_Nome'] = df_vendas['Mes_Ano'].dt.strftime('%B')

    df_vendas = df_vendas.drop_duplicates(subset='Cod_Reserva', keep='first')

    df_vendas = df_vendas[['Data_Venda', 'Cod_Reserva', 'Nome_Parceiro', 'Nome_Estabelecimento_Origem', 'Total Paxs', 'Mes_Ano', 'Mes', 'Ano', 'Mes_Nome']]

    df_vendas_agrupado = df_vendas.groupby(['Cod_Reserva', 'Nome_Estabelecimento_Origem']).agg({'Total Paxs': 'max', 'Mes_Ano': 'first', 'Data_Venda': 'first', 'Mes': 'first', 'Ano': 'first', 
                                                                                                'Mes_Nome': 'first'}).reset_index()
    
    return df_vendas_agrupado

def gerar_df_filtrado(df_vendas_agrupado, df_paxs_in):

    df_filtrado = df_vendas_agrupado[~df_vendas_agrupado['Cod_Reserva'].isin(df_paxs_in['Reserva'])]

    df_filtrado['Cod_Reserva'] = df_filtrado['Cod_Reserva'].str.replace(r'\.\d+$', '', regex=True)

    df_filtrado = df_filtrado[~df_filtrado['Cod_Reserva'].isin(df_paxs_in['Reserva'])]

    return df_filtrado

def colher_ano_mes_selecao(df_paxs_in):

    lista_anos = df_paxs_in['Ano'].dropna().unique().tolist()

    lista_mes = [valor for valor in st.session_state.meses_disponiveis.keys() if valor in df_paxs_in['Mes_Nome'].unique()]

    ano_selecao = st.multiselect('Selecione o Ano:', options=lista_anos, default=[], key='paxs_real_001')

    mes_selecao = st.multiselect('Selecione o Mês', options=lista_mes, default=[], key='paxs_real_002')

    return ano_selecao, mes_selecao

def gerar_df_filtrado_hotel(df_filtrado):

    df_filtrado_data = df_filtrado[(df_filtrado['Ano'].isin(ano_selecao)) & (df_filtrado['Mes_Nome'].isin(mes_selecao))]

    df_filtrado_hotel = df_filtrado_data.groupby('Nome_Estabelecimento_Origem').agg({'Total Paxs': 'sum', 'Mes': 'first', 'Ano': 'first'}).reset_index()

    return df_filtrado_hotel

def gerar_df_top5_operadora(df_paxs_in):

    df_top5_operadora = df_paxs_in.groupby(['Parceiro']).agg({'Total_Paxs': 'sum',}).reset_index()

    df_top5_operadora = df_top5_operadora.query("Parceiro != 'LUCK JOÃO PESSOA - PDV'").nlargest(5, 'Total_Paxs')

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
        x=df_top15_hotel['Nome_Estabelecimento_Origem'],
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

st.set_page_config(layout='wide')

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

st.session_state.lista_colunas_numero_df_historico_vendedor = ['Valor', 'Meta', 'Paxs_Total']

st.session_state.lista_colunas_data_df_historico_vendedor = ['Data']

st.session_state.id_gsheet_reembolsos = '1dmcVUq7Bl_ipxPyxY8IcgxT7dCmTh_FLxYJqGigoSb0'

st.session_state.lista_colunas_numero_df_reembolsos = ['Valor_Total']

st.session_state.lista_colunas_data_df_reembolsos = ['Data_venc']

st.session_state.meses_disponiveis = {'Janeiro': 1, 'Fevereiro': 2, 'Março': 3, 'Abril': 4, 'Maio': 5, 'Junho': 6, 'Julho': 7, 'Agosto': 8, 'Setembro': 9, 'Outubro': 10, 'Novembro': 11, 
                                        'Dezembro': 12}

st.session_state.setores_desejados_gerencial = ['EVENTOS', 'GRUPOS', 'GUIA', 'HOTEL VENDAS', 'PDV', 'VENDAS ONLINE']

st.session_state.setores_desejados_historico_por_vendedor = ['PDV', 'GUIA', 'HOTEL VENDAS', 'VENDAS ONLINE']

st.session_state.combo_luck = ['CATAMARÃ DO FORRÓ', 'CITY TOUR', 'EMBARCAÇAO - CATAMARÃ DO FORRÓ ',  'EMBARCAÇÃO - ILHA DE AREIA VERMELHA', 'EMBARCAÇÃO - PASSEIO PELO RIO PARAÍBA', 
                                'ILHA DE AREIA VERMELHA', 'EMBARCAÇÃO - PISCINAS DO EXTREMO ORIENTAL', 'ENTARDECER NA PRAIA DO JACARÉ ', 'LITORAL NORTE COM ENTARDECER NA PRAIA DO JACARÉ', 
                                'PISCINAS DO EXTREMO ORIENTAL', 'PRAIAS DA COSTA DO CONDE']

st.title('Paxs IN')

st.divider()

if any(key not in st.session_state for key in ['df_config', 'df_metas', 'df_vendas_final', 'df_paxs_in']):

    with st.spinner('Puxando reembolsos, configurações, histórico...'):

        puxar_aba_simples(st.session_state.id_gsheet_metas_vendas, 'Configurações Vendas', 'df_config')

        gerar_df_metas()

    with st.spinner('Puxando vendas, ranking, guias IN e paxs IN do Phoenix...'):

        st.session_state.df_vendas_final = gerar_df_vendas_final()

        gerar_df_paxs_in()

locale.setlocale(locale.LC_TIME, 'pt_BR.UTF-8')

df_vendas_agrupado = criar_df_vendas_agrupado()

df_paxs_in = st.session_state.df_paxs_in.copy()

df_paxs_in['Mes_Nome'] = df_paxs_in['Mes_Ano'].dt.strftime('%B')

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
