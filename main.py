import pandas as pd
import re
from datetime import datetime
import os
from typing import Dict, Any

# --- Importações para API e Banco de Dados ---
from fastapi import FastAPI, Depends, HTTPException, status, Security
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, HttpUrl
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection

# --- Importações do Selenium (Scraping) ---
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

# --- CONFIGURAÇÃO DA APLICAÇÃO ---
app = FastAPI(
    title="API de Processamento de Notas Fiscais",
    description="Uma API para extrair e salvar dados de notas fiscais a partir de uma URL.",
    version="1.0.0"
)

# --- CONFIGURAÇÃO DO BANCO DE DADOS ---
# Para segurança, é recomendado usar variáveis de ambiente.
# Você pode criar um arquivo .env e carregar com a biblioteca python-dotenv
from dotenv import load_dotenv
load_dotenv()
DATABASE_URL = os.getenv('DATABASE_URL')
if not DATABASE_URL:
    raise Exception("A variável de ambiente DATABASE_URL não foi definida.")

engine = create_engine(DATABASE_URL)

API_KEY_NAME = "x-api-key" # Nome do cabeçalho que conterá a chave
API_KEY_HEADER = APIKeyHeader(name=API_KEY_NAME, auto_error=True)

# LEIA A CHAVE SECRETA DE UMA VARIÁVEL DE AMBIENTE
SECRET_API_KEY = os.getenv("SECRET_API_KEY")
if not SECRET_API_KEY:
    raise Exception("A variável de ambiente SECRET_API_KEY não foi definida.")

# Função de dependência para verificar a chave
async def get_api_key(api_key: str = Security(API_KEY_HEADER)):
    if api_key == SECRET_API_KEY:
        return api_key
    else:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Could not validate credentials"
        )


# --- FUNÇÕES DE BANCO DE DADOS ---

def inicializar_banco_nuvem():
    """Cria as tabelas no banco de dados se elas não existirem."""
    print("Verificando e inicializando o banco de dados na nuvem...")
    with engine.connect() as conn:
        conn.execute(text('''
            CREATE TABLE IF NOT EXISTS notas_processadas (
                id SERIAL PRIMARY KEY,
                url TEXT NOT NULL UNIQUE,
                nome_estabelecimento TEXT,
                numero_nota TEXT,
                data_emissao_nota TIMESTAMP,
                data_processamento TIMESTAMP NOT NULL
            )
        '''))
        conn.execute(text('''
            CREATE TABLE IF NOT EXISTS historico_precos (
                id SERIAL PRIMARY KEY,
                nota_id INTEGER REFERENCES notas_processadas(id) ON DELETE CASCADE,
                produto TEXT,
                codigo TEXT,
                quantidade REAL,
                unidade TEXT,
                valor_unitario REAL,
                valor_total REAL,
                data_coleta TIMESTAMP
            )
        '''))
        conn.commit()
    print("Banco de dados verificado/inicializado com sucesso.")


# Evento de startup para inicializar o banco quando a API liga
@app.on_event("startup")
def on_startup():
    inicializar_banco_nuvem()


# Dependência para obter a conexão com o banco de dados por requisição
def get_db_conn():
    with engine.connect() as connection:
        yield connection


def url_ja_processada(url: str, conn: Connection) -> bool:
    """Verifica se a URL já existe na tabela de controle."""
    query = text("SELECT id FROM notas_processadas WHERE url = :url")
    resultado = conn.execute(query, {"url": url}).fetchone()
    return resultado is not None


def marcar_url_como_processada(url: str, estabelecimento: str, numero_nota: str, data_emissao: datetime,
                               conn: Connection) -> int:
    """Insere a URL e os dados da nota na tabela de controle e retorna o ID."""
    data_processamento = datetime.now()
    query = text(
        """
        INSERT INTO notas_processadas (url, nome_estabelecimento, numero_nota, data_emissao_nota, data_processamento) 
        VALUES (:url, :est, :num, :data_emissao, :data_proc) RETURNING id
        """
    )
    result = conn.execute(query, {
        "url": url,
        "est": estabelecimento,
        "num": numero_nota,
        "data_emissao": data_emissao,
        "data_proc": data_processamento
    }).fetchone()
    return result[0]


def salvar_dados_no_banco(df: pd.DataFrame, nota_id: int, conn: Connection):
    """Salva o DataFrame no banco de dados da nuvem."""
    if df is None or df.empty:
        print("DataFrame vazio. Nada para salvar.")
        return 0

    df['nota_id'] = nota_id
    df['data_coleta'] = datetime.now()

    df.to_sql(name='historico_precos', con=conn, if_exists='append', index=False)
    print(f"--> SUCESSO: {len(df)} registros salvos no banco na nuvem para a nota_id {nota_id}!")
    return len(df)


# --- FUNÇÕES DE SCRAPING (mantidas do script original) ---

def extrair_dados_completos(html_conteudo: str) -> Dict[str, Any]:
    """Recebe o HTML e extrai os itens, número da nota e data de emissão."""
    soup = BeautifulSoup(html_conteudo, 'lxml')
    tabela = soup.find('table', id='tabResult')
    df_itens = None
    if tabela:
        df_itens = extrair_e_limpar_itens(str(tabela))

    numero_nota = None
    data_emissao = None
    infos_div = soup.find('div', id='infos')
    if infos_div:
        infos_gerais_li = infos_div.find('li', class_='ui-li-static')
        if infos_gerais_li:
            texto_completo = infos_gerais_li.get_text(separator=' ', strip=True)
            match_numero = re.search(r"Número:\s*(\d+)", texto_completo)
            if match_numero:
                numero_nota = match_numero.group(1)
            match_data = re.search(r"Emissão:\s*(\d{2}/\d{2}/\d{4}\s\d{2}:\d{2}:\d{2})", texto_completo)
            if match_data:
                data_emissao_str = match_data.group(1)
                data_emissao = datetime.strptime(data_emissao_str, '%d/%m/%Y %H:%M:%S')

    return {"itens_df": df_itens, "numero_nota": numero_nota, "data_emissao": data_emissao}


def extrair_e_limpar_itens(html_da_tabela: str) -> pd.DataFrame | None:
    """Função auxiliar que processa apenas a tabela de itens."""
    soup = BeautifulSoup(html_da_tabela, 'lxml')
    itens_rows = soup.find_all('tr', id=lambda x: x and x.startswith('Item'))
    if not itens_rows:
        return None
    lista_de_itens = []
    for item_row in itens_rows:
        nome = item_row.find('span', class_='txtTit').text.strip() if item_row.find('span', class_='txtTit') else 'N/A'
        codigo_bruto = item_row.find('span', class_='RCod').text.strip() if item_row.find('span', class_='RCod') else ''
        match = re.search(r'\d+', codigo_bruto.replace('\n', ''))
        codigo = match.group(0) if match else 'N/A'
        qtd_bruto = item_row.find('span', class_='Rqtd').text.strip() if item_row.find('span', class_='Rqtd') else ''
        qtd = qtd_bruto.split(':')[-1].replace(',', '.').strip()
        un_bruto = item_row.find('span', class_='RUN').text.strip() if item_row.find('span', class_='RUN') else ''
        unidade = un_bruto.split(':')[-1].strip()
        vl_unit_bruto = item_row.find('span', class_='RvlUnit').text.strip() if item_row.find('span',
                                                                                              class_='RvlUnit') else ''
        vl_unit = vl_unit_bruto.split(':')[-1].replace(',', '.').strip()
        vl_total = item_row.find('span', class_='valor').text.strip().replace(',', '.') if item_row.find('span',
                                                                                                         class_='valor') else '0.0'

        lista_de_itens.append({
            'produto': nome, 'codigo': codigo, 'quantidade': float(qtd) if qtd else 0.0,
            'unidade': unidade, 'valor_unitario': float(vl_unit) if vl_unit else 0.0,
            'valor_total': float(vl_total) if vl_total else 0.0
        })
    return pd.DataFrame(lista_de_itens)


def buscar_dados_da_url(url: str) -> Dict[str, Any] | None:
    """Função principal de scraping com Selenium."""
    service = Service(ChromeDriverManager().install())
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument("window-size=1920,1080")

    driver = webdriver.Chrome(service=service, options=options)
    dados_completos = None
    try:
        print(f"Acessando a URL: {url}")
        driver.get(url)
        driver.implicitly_wait(10)
        conteudo_container = driver.find_element(By.CSS_SELECTOR, 'div.ui-content')
        html_container = conteudo_container.get_attribute('outerHTML')
        dados_completos = extrair_dados_completos(html_container)
    except Exception as e:
        print(f"Erro durante o scraping da URL {url}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Falha no Selenium ao acessar a URL: {e}"
        )
    finally:
        driver.quit()
    return dados_completos


# --- MODELO DE DADOS PARA A REQUISIÇÃO ---

class NotaRequest(BaseModel):
    url: HttpUrl  # Pydantic valida se é uma URL válida
    nome_estabelecimento: str


# --- ENDPOINT DA API ---

@app.post("/processar-nota", status_code=status.HTTP_201_CREATED)
def processar_nota_fiscal(
        request: NotaRequest,
        conn: Connection = Depends(get_db_conn),
        api_key: str = Depends(get_api_key)
):
    """
    Recebe a URL de uma nota fiscal e o nome do estabelecimento,
    faz o scraping dos dados, e os salva no banco de dados.
    """
    url_str = str(request.url)
    print("-" * 40)
    print(f"Recebida requisição para processar URL: {url_str[:70]}...")

    # A transação garante que todas as operações de banco de dados ou funcionam ou falham juntas
    with conn.begin() as transaction:
        try:
            if url_ja_processada(url_str, conn):
                print(f"--> AVISO: URL já processada anteriormente.")
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Esta URL de nota fiscal já foi processada."
                )

            print("--> URL nova. Iniciando scraping...")
            dados_extraidos = buscar_dados_da_url(url_str)

            if not dados_extraidos or dados_extraidos.get("itens_df") is None or dados_extraidos["itens_df"].empty:
                print("--> FALHA: Scraping não retornou dados.")
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail="Não foi possível extrair os itens da nota da URL fornecida. Verifique o link ou o layout da página."
                )

            df_produtos = dados_extraidos["itens_df"]
            num_nota = dados_extraidos["numero_nota"]
            data_emissao_nota = dados_extraidos["data_emissao"]

            print(f"--> Nota encontrada: Nº {num_nota} | Data: {data_emissao_nota}")

            # Salva o registro da nota principal e obtém o ID
            nova_nota_id = marcar_url_como_processada(
                url_str, request.nome_estabelecimento, num_nota, data_emissao_nota, conn
            )

            # Salva os produtos da nota
            registros_salvos = salvar_dados_no_banco(df_produtos, nova_nota_id, conn)

            # transaction.commit() é chamado automaticamente ao sair do bloco 'with'

            return {
                "status": "sucesso",
                "mensagem": f"Nota fiscal processada e {registros_salvos} itens salvos.",
                "nota_id": nova_nota_id,
                "numero_nota": num_nota,
                "estabelecimento": request.nome_estabelecimento
            }

        except HTTPException:
            transaction.rollback()  # Desfaz a transação em caso de erro HTTP conhecido
            raise  # Re-levanta a exceção para que o FastAPI a capture
        except Exception as e:
            transaction.rollback()  # Desfaz a transação em caso de erro inesperado
            print(f"Ocorreu um erro inesperado: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Ocorreu um erro interno no servidor: {e}"
            )


@app.get("/", include_in_schema=False)
def root():
    return {"message": "API de Processamento de Notas Fiscais está no ar. Acesse /docs para a documentação."}