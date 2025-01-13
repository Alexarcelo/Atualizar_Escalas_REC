import streamlit as st
import mysql.connector
import decimal
import pandas as pd
import requests
import gspread 
from google.cloud import secretmanager 
from google.oauth2.service_account import Credentials
from google.oauth2 import service_account
import json

def gerar_df_phoenix(vw_name, base_luck):

    config = {
        'user': 'user_automation_jpa',
        'password': 'luck_jpa_2024',
        'host': 'comeia.cixat7j68g0n.us-east-1.rds.amazonaws.com',
        'database': base_luck
        }

    conexao = mysql.connector.connect(**config)
    cursor = conexao.cursor()
    request_name = f'SELECT * FROM {vw_name}'
    cursor.execute(request_name)
    resultado = cursor.fetchall()
    cabecalho = [desc[0] for desc in cursor.description]
    cursor.close()
    conexao.close()
    df = pd.DataFrame(resultado, columns=cabecalho)
    df = df.applymap(lambda x: float(x) if isinstance(x, decimal.Decimal) else x)

    return df

def puxar_aba_simples(id_gsheet, nome_aba, nome_df):

    project_id = "grupoluck"
    secret_id = "cred-luck-aracaju"
    secret_client = secretmanager.SecretManagerServiceClient()
    secret_name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = secret_client.access_secret_version(request={"name": secret_name})
    secret_payload = response.payload.data.decode("UTF-8")
    credentials_info = json.loads(secret_payload)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    credentials = Credentials.from_service_account_info(credentials_info, scopes=scopes)
    client = gspread.authorize(credentials)

    spreadsheet = client.open_by_key(id_gsheet)
    
    sheet = spreadsheet.worksheet(nome_aba)

    sheet_data = sheet.get_all_values()

    st.session_state[nome_df] = pd.DataFrame(sheet_data[1:], columns=sheet_data[0])

def verificar_cadastros_veic_mot_guias():

    lista_veiculos_a_atualizar = st.session_state.df_escalas_atualizar['Veiculo'].unique().tolist()

    lista_veiculos_phoenix = st.session_state.df_escalas['Veiculo'].unique().tolist()

    lista_veiculos_nao_cadastrados = list(set(lista_veiculos_a_atualizar) - set(lista_veiculos_phoenix))

    lista_motoristas_a_atualizar = st.session_state.df_escalas_atualizar['Motorista'].unique().tolist()

    lista_motoristas_phoenix = st.session_state.df_escalas['Motorista'].unique().tolist()

    lista_motoristas_nao_cadastrados = list(set(lista_motoristas_a_atualizar) - set(lista_motoristas_phoenix))

    lista_guias_a_atualizar = st.session_state.df_escalas_atualizar['Guia'].unique().tolist()

    lista_guias_phoenix = st.session_state.df_escalas['Guia'].unique().tolist()

    lista_guias_nao_cadastrados = list(set(lista_guias_a_atualizar) - set(lista_guias_phoenix))

    if len(lista_veiculos_nao_cadastrados)>0:

        st.error(f'Os veículos {", ".join(lista_veiculos_nao_cadastrados)} não existem no Phoenix. Precisa ajustar a nomenclatura na planilha e tentar novamente')

    if len(lista_motoristas_nao_cadastrados)>0:

        st.error(f'Os motoristas {", ".join(lista_motoristas_nao_cadastrados)} não existem no Phoenix. Precisa ajustar a nomenclatura na planilha e tentar novamente')

    if len(lista_guias_nao_cadastrados)>0:

        st.error(f'Os guias {", ".join(lista_guias_nao_cadastrados)} não existem no Phoenix. Precisa ajustar a nomenclatura na planilha e tentar novamente')

    if len(lista_veiculos_nao_cadastrados)>0 or len(lista_motoristas_nao_cadastrados)>0 or len(lista_guias_nao_cadastrados)>0:

        st.stop()

def update_scale(payload):

    try:
        response = requests.post(st.session_state.base_url_post, json=payload, verify=False)
        response.raise_for_status()
        return 'Escala atualizada com sucesso!'
    except requests.RequestException as e:
        st.error(f"Ocorreu um erro: {e}")
        return 'Erro ao atualizar a escala'

def get_novo_codigo(reserve_service_id):
    novo_codigo = st.session_state.df_escalas[
        (st.session_state.df_escalas['ID Servico'] == reserve_service_id) 
    ]
    if novo_codigo.empty:
        return 'Escala não encontrada'
    return novo_codigo['Escala'].values[0]

def inserir_novas_escalas_drive(df_itens_faltantes, id_gsheet, nome_aba):

    project_id = "grupoluck"
    secret_id = "cred-luck-aracaju"
    secret_client = secretmanager.SecretManagerServiceClient()
    secret_name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = secret_client.access_secret_version(request={"name": secret_name})
    secret_payload = response.payload.data.decode("UTF-8")
    credentials_info = json.loads(secret_payload)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    credentials = Credentials.from_service_account_info(credentials_info, scopes=scopes)
    client = gspread.authorize(credentials)
    
    spreadsheet = client.open_by_key(id_gsheet)

    sheet = spreadsheet.worksheet(nome_aba)

    sheet.batch_clear(["A2:Z100000"])

    data = df_itens_faltantes.values.tolist()
    sheet.update('A2', data)

st.set_page_config(layout='wide')

if not 'base_luck' in st.session_state:

    st.session_state.base_luck = 'test_phoenix_recife'

if not 'base_url_post' in st.session_state:

    st.session_state.base_url_post = 'https://driverrecife.phoenix.comeialabs.com/scale/roadmap/allocate'

if not 'id_gsheet' in st.session_state:

    st.session_state.id_gsheet = '1tK_tUWk5gDFcv0vKHviWYax0r3njkLldwAXx6AFxUhs'

if not 'df_escalas' in st.session_state:

    with st.spinner('Puxando dados do Phoenix...'):

        st.session_state.df_escalas = gerar_df_phoenix('vw_scales', st.session_state.base_luck)

row0 = st.columns(1)

st.title('Atualizar Escalas')

st.divider()

row1 = st.columns(3)

with row1[1]:

    atualizar_phoenix = st.button('Atualizar Dados Phoenix')

if atualizar_phoenix:

    with st.spinner('Puxando dados do Phoenix...'):

        st.session_state.df_escalas = gerar_df_phoenix('vw_scales', st.session_state.base_luck)

with row1[0]:

    atualizar_escalas = st.button('Atualizar Escalas')

if atualizar_escalas:

    with st.spinner('Puxando dados do Google Drive...'):

        puxar_aba_simples(st.session_state.id_gsheet, 'Atualizar Escalas', 'df_escalas_atualizar')

        st.session_state.df_escalas_atualizar = st.session_state.df_escalas_atualizar[st.session_state.df_escalas_atualizar['Escala Nova']==''].reset_index(drop=True)

        if len(st.session_state.df_escalas_atualizar)==0:

            st.error('Não existem escalas pra atualizar')

            st.stop()

        verificar_cadastros_veic_mot_guias()

    df_escalas_a_atualizar = st.session_state.df_escalas[st.session_state.df_escalas['Escala'].isin(st.session_state.df_escalas_atualizar['Escala'].unique())].reset_index(drop=True)

    df_id_veiculo = st.session_state.df_escalas[pd.notna(st.session_state.df_escalas['Veiculo'])][['Veiculo', 'ID Veiculo']].drop_duplicates().reset_index(drop=True)

    df_id_motorista = st.session_state.df_escalas[pd.notna(st.session_state.df_escalas['Motorista'])][['Motorista', 'ID Motorista']].drop_duplicates().reset_index(drop=True)

    df_id_guia = st.session_state.df_escalas[pd.notna(st.session_state.df_escalas['Guia'])][['Guia', 'ID Guia']].drop_duplicates().reset_index(drop=True)

    escalas_para_atualizar = []

    for index in range(len(st.session_state.df_escalas_atualizar)):

        escala = st.session_state.df_escalas_atualizar.at[index, 'Escala']

        veiculo = st.session_state.df_escalas_atualizar.at[index, 'Veiculo']

        motorista = st.session_state.df_escalas_atualizar.at[index, 'Motorista']

        guia = st.session_state.df_escalas_atualizar.at[index, 'Guia']

        df_ref = df_escalas_a_atualizar[df_escalas_a_atualizar['Escala']==escala]

        if len(df_ref)>0:

            id_servicos = [int(item) for item in df_ref['ID Servico'].tolist()]
    
            date_str = df_ref['Data da Escala'].values[0].strftime('%Y-%m-%d')
    
            id_veiculo = int(df_id_veiculo[df_id_veiculo['Veiculo']==veiculo]['ID Veiculo'].iloc[0])
    
            id_motorista = int(df_id_motorista[df_id_motorista['Motorista']==motorista]['ID Motorista'].iloc[0])
    
            id_guia = int(df_id_guia[df_id_guia['Guia']==guia]['ID Guia'].iloc[0])
    
            payload = {
                    "date": date_str,
                    "vehicle_id": id_veiculo,
                    "driver_id": id_motorista,
                    "guide_id": id_guia,
                    "reserve_service_ids": id_servicos,
                }
            
            escalas_para_atualizar.append(payload)

        else:

            st.error(f'A escala {escala} não foi encontrada.')

            st.stop()

    placeholder = st.empty()
    placeholder.dataframe(escalas_para_atualizar)
    for escala in escalas_para_atualizar:
        escala_atual = escala.copy()
        if escala_atual['guide_id'] == None:
            escala_atual.pop('guide_id')
        if escala_atual['driver_id'] == None:
            escala_atual.pop('driver_id')
        if escala_atual['vehicle_id'] == None:
            escala_atual.pop('vehicle_id')
        status = update_scale(escala_atual)
        escala['status'] = status
        placeholder.dataframe(escalas_para_atualizar)

    with st.spinner('Buscando novos códigos de escalas...'):

        st.session_state.df_escalas = gerar_df_phoenix('vw_scales', st.session_state.base_luck)

    contador=0

    for escala in escalas_para_atualizar:
        novo_codigo = get_novo_codigo(escala['reserve_service_ids'][0])
        escala['novo_codigo'] = novo_codigo
        placeholder.dataframe(escalas_para_atualizar)
        st.session_state.df_escalas_atualizar.at[contador, 'Escala Nova'] = novo_codigo
        contador+=1

    with st.spinner('Inserindo novos códigos de escala no Google Drive...'):

        inserir_novas_escalas_drive(st.session_state.df_escalas_atualizar, st.session_state.id_gsheet, 'Atualizar Escalas')
