import streamlit as st
import pandas as pd
import mysql.connector
import decimal
import gspread
from google.oauth2 import service_account
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

    st.session_state.df_reembolsos['Ano'] = pd.to_datetime(st.session_state.df_reembolsos['Data_venc']).dt.year
    
    st.session_state.df_reembolsos['Mes'] = pd.to_datetime(st.session_state.df_reembolsos['Data_venc']).dt.month

    st.session_state.df_reembolsos['Mes_Ano'] = pd.to_datetime(st.session_state.df_reembolsos['Data_venc']).dt.to_period('M')

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

    return df_vendas

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

def gerar_df_historico():

    puxar_aba_simples(st.session_state.id_gsheet_metas_vendas, 'BD - Historico', 'df_historico')

    tratar_colunas_numero_df(st.session_state.df_historico, st.session_state.lista_colunas_numero_df_historico)

    tratar_colunas_data_df(st.session_state.df_historico, st.session_state.lista_colunas_data_df_historico)

    st.session_state.df_historico['Ano'] = pd.to_datetime(st.session_state.df_historico['Data']).dt.year
    
    st.session_state.df_historico['Mes'] = pd.to_datetime(st.session_state.df_historico['Data']).dt.month
    
    st.session_state.df_historico['Mes_Ano'] = pd.to_datetime(st.session_state.df_historico['Data']).dt.to_period('M')

def formatar_moeda(valor):

    return format_currency(valor, 'BRL', locale='pt_BR')

def gerar_df_vendas_reembolsos():

    reembolso_filtrado = st.session_state.df_reembolsos.groupby(['Vendedor', 'Mes_Ano'])['Valor_Total'].sum().reset_index()

    vendas_filtrado = st.session_state.df_vendas_final.groupby(['Vendedor', 'Setor', 'Mes_Ano', 'Ano', 'Mes'])['Valor_Venda'].sum().reset_index()

    df_vendas = pd.merge(vendas_filtrado, reembolso_filtrado, on=['Vendedor', 'Mes_Ano'], how='left')

    df_vendas['Valor_Total'].fillna(0, inplace=True)

    df_vendas['Valor_Real'] = df_vendas['Valor_Venda'] - df_vendas['Valor_Total']

    return df_vendas

def gerar_df_agrupado(df_vendas):

    df_paxs_in = st.session_state.df_paxs_in.groupby(['Mes_Ano'])['Total_Paxs_Periodo'].sum().reset_index()

    df_historico_setor = st.session_state.df_historico.groupby(['Setor', 'Mes_Ano']).agg({'Valor_Venda': 'sum', 'Paxs ADT': 'mean'}).reset_index()

    df_vendas_setor = df_vendas.groupby(['Setor', 'Mes_Ano'])['Valor_Real'].sum().reset_index()

    df_agrupado = pd.merge(df_historico_setor, df_vendas_setor, on=['Setor', 'Mes_Ano'], how='outer')

    df_agrupado = pd.merge(df_agrupado, df_paxs_in[['Mes_Ano', 'Total_Paxs_Periodo']], on='Mes_Ano', how='left')

    df_agrupado['Paxs'] = df_agrupado[['Paxs ADT', 'Total_Paxs_Periodo']].max(axis=1)

    df_agrupado = df_agrupado.drop(columns=['Paxs ADT', 'Total_Paxs_Periodo'])

    df_agrupado['Valor_Venda'] = df_agrupado['Valor_Venda'].fillna(0)

    df_agrupado['Valor_Real'] = df_agrupado['Valor_Real'].fillna(0)

    df_agrupado['Valor_Total'] = df_agrupado['Valor_Venda'] + df_agrupado['Valor_Real']

    df_agrupado['Paxs'] = df_agrupado['Paxs'].astype(float)

    df_agrupado['Ticket_Medio'] = df_agrupado['Valor_Total'] / df_agrupado['Paxs']

    df_agrupado['Mes_Ano'] = df_agrupado['Mes_Ano'].astype(str)

    return df_agrupado

def plotar_graficos_linha_por_setor(setores, df_agrupado):

    colunas = st.columns(2)

    i = 0

    for setor in setores:

        df_setor = df_agrupado[df_agrupado['Setor'] == setor]

        fig = go.Figure()
        
        # Adicionar a linha do Valor Total
        fig.add_trace(go.Bar(
            x=df_setor['Mes_Ano'],
            y=df_setor['Valor_Total'],
            #mode='lines+markers+text',
            name='Valor Total',
            text=df_setor['Valor_Total'].apply(formatar_moeda),
            textposition='outside',
            textfont=dict(size=10, color='green'),
            #line=dict(color='#047c6c', shape='spline'),
            #marker=dict(size=6)
            marker=dict(color='#047c6c')
        ))

        # Adicionando a linha do Ticket Médio no eixo Y secundário
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

        # Atualizar o layout para incluir Y2
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
                #range=[-500, 300],
                titlefont=dict(color='orange'),
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
        
        fig = px.bar(df_mes, x='Setor', y='Valor_Total', color='Ano',
                    title=f'Valor Total por Setor Ano a Ano - Ref. {mes_nome}',
                    labels={'Valor_Total': 'Valor Total', 'Setor': 'Setor'},
                    text=df_mes['Valor_Total'].apply(formatar_moeda),
                    color_discrete_sequence=['#047c6c', '#3CB371', '#90EE90'],
                    barmode='group')
        fig.update_traces(
            textposition='outside',
            textfont=dict(size=10, color='green')
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

    fig_ano = px.bar(df_vendas_anual, x='Setor', y='Valor_Total', color='Ano',
                title=f'Valor Total por Setor Ano a Ano',
                labels={'Valor_Total': 'Valor Total', 'Setor': 'Setor'},
                text=df_vendas_anual['Valor_Total'].apply(formatar_moeda),
                color_discrete_sequence=['#047c6c', '#3CB371', '#90EE90'],
                barmode='group',
                category_orders={"Ano": sorted(df_vendas_anual['Ano'].unique())}  # Define a ordem dos anos
                )
    fig_ano.update_traces(
        textposition='outside',
        textfont=dict(size=10, color='green')
    )
    fig_ano.add_trace(
        go.Scatter(
            x=df_vendas_anual['Setor'], 
            y=df_vendas_anual['Variacao_Anual'], 
            mode='lines+markers+text', 
            line=dict(color='orange', width=1),
            name='Variação %', 
            line_shape='spline',  # Suaviza a linha
            text=[f"{round(val, 2)}%" for val in df_vendas_anual['Variacao_Anual']],
            textfont=dict(size=12, color='orange'),
            yaxis='y2',
            textposition='top center',
    )
    )
    fig_ano.update_layout(
        bargap=0.2,
        bargroupgap=0.1,
        xaxis=dict(showgrid=False),
        yaxis=dict(title='Valor Total', showgrid=False, range=[0, df_vendas_anual['Valor_Total'].max() * 1.5]),
        yaxis2=dict(
            title='Variação (%)',
            overlaying='y',
            side='right',  # Coloca o eixo Y2 à direita
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
                textfont=dict(size=10, color='green')
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
        textfont=dict(size=12, color='rgb(4,124,108)'),
    ))

    fig_tm.add_trace(go.Scatter(
        x=df_filtro_receita['Mes_Ano'],
        y=df_filtro_receita['Ticket_Medio'] * 10000,  # Ajuste de escala para visualização
        name='Ticket Médio',
        mode='lines+markers+text',
        line=dict(color='rgb(4,124,108)', width=1, shape='spline'),
        textposition='top center',
        text=df_filtro_receita['Ticket_Medio'].apply(formatar_moeda),
        textfont=dict(size=10, color='rgb(4,124,108)')
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

st.title('Gerencial - Ano a Ano')

st.divider()

if not 'df_vendas' in st.session_state:

    with st.spinner('Puxando vendas manuais, reembolsos, metas de vendedores, metas de setores, configurações, histórico...'):

        gerar_df_vendas_manuais()

        puxar_aba_simples(st.session_state.id_gsheet_metas_vendas, 'Configurações Vendas', 'df_config')

        gerar_df_historico()

        gerar_df_reembolsos()

    with st.spinner('Puxando vendas, ranking, guias IN e paxs IN do Phoenix...'):

        gerar_df_vendas()

        st.session_state.df_vendas_final = gerar_df_vendas_final()

        gerar_df_paxs_in()

df_vendas = gerar_df_vendas_reembolsos()

df_agrupado = gerar_df_agrupado(df_vendas)

df_filtrado = df_agrupado[df_agrupado['Setor'].isin(st.session_state.setores_desejados_gerencial)]

setores = df_filtrado['Setor'].unique()

plotar_graficos_linha_por_setor(setores, df_agrupado)

df_vendas_mensal = df_vendas.groupby(['Mes', 'Ano', 'Setor'], as_index=False)['Valor_Total'].sum()

plotar_graficos_barra_valor_total_por_setor(df_vendas_mensal)

df_vendas_anual = df_vendas.groupby(['Ano', 'Setor'], as_index=False)['Valor_Total'].sum()

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
