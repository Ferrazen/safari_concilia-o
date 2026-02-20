import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from datetime import date
from io import BytesIO

# =========================
# CONFIGURAÇÃO INICIAL
# =========================
st.set_page_config(page_title="Safári ERP Financeiro", layout="wide")

engine = create_engine("sqlite:///safari.db", echo=False)
inspector = inspect(engine)

col_logo, col_titulo = st.columns([1, 5])

with col_logo:
    st.image("safari_logo.png", width=110)

with col_titulo:
    st.markdown(
        "<h1 style='margin-bottom:0;'>Safári - Sistema Financeiro</h1>",
        unsafe_allow_html=True
    )


menu = st.sidebar.radio(
    "Menu",
    [
        "Dashboard",
        "Plano de Contas",
        "Lançamentos",
        "DFC - Caixa",
        "Auditoria DFC",
        "DRE - Competência",
        "Admin",
    ],
)

# =========================
# CSS
# =========================
st.markdown(
    """
<style>
.linha {
    display: flex;
    justify-content: space-between;
    gap: 14px;
    padding: 6px 10px;
    border-bottom: 1px solid rgba(120,120,120,0.15);
    align-items: center;
}
.linha:hover { background: rgba(120,120,120,0.06); }
.linha .desc { flex: 1; }
.linha .valor { min-width: 180px; text-align: right; font-variant-numeric: tabular-nums; }
.titulo-bloco {
    font-weight: 700;
    padding: 6px 10px;
    margin-top: 8px;
    background: rgba(120,120,120,0.08);
    border-radius: 10px;
}
.badge {
    display: inline-block;
    padding: 2px 8px;
    border-radius: 999px;
    background: rgba(120,120,120,0.10);
    font-size: 12px;
    margin-left: 8px;
}
.smallmuted { color: rgba(120,120,120,0.85); font-size: 12px; }
</style>
""",
    unsafe_allow_html=True,
)

# =========================
# UTILITÁRIOS
# =========================
STATUS_SAIDA = ["A pagar", "Atrasado", "Renegociado", "Pago"]
STATUS_ENTRADA = ["A receber", "Atrasado", "Renegociado", "Recebido"]

def status_opcoes_por_natureza(natureza: str):
    n = (natureza or "").strip()
    return STATUS_ENTRADA if n == "Entrada" else STATUS_SAIDA

def default_status_por_natureza(natureza: str):
    n = (natureza or "").strip()
    return "A receber" if n == "Entrada" else "A pagar"

def fmt_brl(v: float) -> str:
    try:
        v = float(v)
    except Exception:
        return "R$ 0,00"
    neg = v < 0
    v = abs(v)
    s = f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"(R$ {s})" if neg else f"R$ {s}"

def atualizar_atrasados():
    hoje = date.today().isoformat()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
            UPDATE lancamentos
               SET status = 'Atrasado'
             WHERE date(data_pagamento) < date(:hoje)
               AND status IN ('A pagar', 'A receber')
        """
            ),
            {"hoje": hoje},
        )

def normalizar_codigo(cod: str) -> str:
    cod = (str(cod) if cod is not None else "").strip()
    return cod

def codigo_blocos(cod: str):
    p = normalizar_codigo(cod).split(".")
    if len(p) < 4:
        p = p + ["00"] * (4 - len(p))
    return p[:4]

def codigo_pais(cod: str):
    a, b, c, d = codigo_blocos(cod)
    pais = []
    if d != "00":
        pais.append(f"{a}.{b}.{c}.00")
    if c != "00":
        pais.append(f"{a}.{b}.00.00")
    if b != "00":
        pais.append(f"{a}.00.00.00")
    return pais

def eh_sintetico(cod: str) -> bool:
    a, b, c, d = codigo_blocos(cod)
    return d == "00"

# =========================
# CONFIGURAÇÕES
# =========================
def ensure_config_table():
    with engine.begin() as conn:
        conn.execute(
            text(
                """
            CREATE TABLE IF NOT EXISTS configuracoes (
                chave TEXT PRIMARY KEY,
                valor TEXT
            )
        """
            )
        )

def set_config(chave: str, valor: str):
    ensure_config_table()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
            INSERT OR REPLACE INTO configuracoes (chave, valor)
            VALUES (:chave, :valor)
        """
            ),
            {"chave": chave, "valor": valor},
        )

def get_config(chave: str, default: str) -> str:
    ensure_config_table()
    df_cfg = pd.read_sql(
        """
        SELECT valor
        FROM configuracoes
        WHERE chave = :chave
    """,
        engine,
        params={"chave": chave},
    )
    if df_cfg.empty:
        return default
    return str(df_cfg["valor"].iloc[0])

def get_config_bool(chave: str, default: bool) -> bool:
    v = get_config(chave, "True" if default else "False")
    return str(v) == "True"

# =========================
# TRANSFORMAR PLANO
# =========================
def transformar_plano(df_original: pd.DataFrame) -> pd.DataFrame:
    df = df_original.copy()
    df.columns = ["descricao", "codigo"]

    df = df.dropna(subset=["codigo"])
    df["codigo"] = df["codigo"].astype(str).str.strip()

    df["natureza"] = df["codigo"].apply(lambda x: "Entrada" if str(x).startswith("1") else "Saída")
    df["tipo"] = df["natureza"].apply(lambda x: "Receita" if x == "Entrada" else "Despesa")

    df["grupo_dre"] = df["tipo"]
    df["grupo_dfc"] = "Operacional"

    df["aceita_lancamento"] = df["codigo"].apply(lambda cod: not eh_sintetico(cod))

    return df[
        [
            "codigo",
            "descricao",
            "tipo",
            "natureza",
            "grupo_dre",
            "grupo_dfc",
            "aceita_lancamento",
        ]
    ]

# =========================
# CRIAR TABELAS
# =========================
with engine.begin() as conn:
    conn.execute(
        text(
            """
        CREATE TABLE IF NOT EXISTS plano_contas (
            codigo TEXT,
            descricao TEXT,
            tipo TEXT,
            natureza TEXT,
            grupo_dre TEXT,
            grupo_dfc TEXT,
            aceita_lancamento INTEGER
        )
    """
        )
    )

    conn.execute(
        text(
            """
        CREATE TABLE IF NOT EXISTS lancamentos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data_competencia TEXT,
            data_pagamento TEXT,
            valor FLOAT,
            status TEXT,
            plano_conta_id TEXT,
            centro_custo TEXT,
            unidade TEXT,
            projeto TEXT,
            observacao TEXT
        )
    """
        )
    )

    conn.execute(
        text(
            """
        CREATE TABLE IF NOT EXISTS saldos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data_referencia TEXT,
            tipo TEXT,
            valor FLOAT,
            observacao TEXT
        )
    """
        )
    )

ensure_config_table()

# =========================
# MENU PRINCIPAL
# =========================

if menu == "Dashboard":
    st.header("📊 Visão Geral")
    st.subheader("Bem-vindo ao Safári ERP Financeiro!")
    col1, col2, col3 = st.columns(3)
    with col1:
        df_lanc = pd.read_sql("SELECT COUNT(*) as total FROM lancamentos", engine)
        total_lanc = int(df_lanc["total"].iloc[0]) if not df_lanc.empty else 0
        st.metric("📋 Total de Lançamentos", total_lanc)
    with col2:
        df_saldos = pd.read_sql("SELECT COUNT(*) as total FROM saldos", engine)
        total_saldos = int(df_saldos["total"].iloc[0]) if not df_saldos.empty else 0
        st.metric("💼 Saldos Registrados", total_saldos)
    with col3:
        df_contas = pd.read_sql("SELECT COUNT(*) as total FROM plano_contas", engine)
        total_contas = int(df_contas["total"].iloc[0]) if not df_contas.empty else 0
        st.metric("📂 Contas do Plano", total_contas)
    st.divider()
    st.info("👈 Use o menu lateral para navegar.")

elif menu == "Plano de Contas":
    st.header("📂 Importar Plano de Contas")
    arquivo = st.file_uploader("Envie seu Excel original", type=["xlsx"])
    if arquivo:
        df_original = pd.read_excel(arquivo)
        df_transformado = transformar_plano(df_original)
        st.subheader("Preview do Plano Transformado")
        st.dataframe(df_transformado, use_container_width=True)
        if st.button("Salvar no Banco", type="primary"):
            df_transformado.to_sql("plano_contas", engine, if_exists="replace", index=False)
            st.success("✅ Plano de contas importado com sucesso!")
            st.rerun()

elif menu == "Lançamentos":
    st.header("🧾 Contas a Pagar / Receber (Lançamentos + Importação)")

    if "plano_contas" not in inspector.get_table_names():
        st.error("Importe o Plano de Contas primeiro.")
        st.stop()

    atualizar_atrasados()

    # Inicializar estado
    if "lanc_secao" not in st.session_state:
        st.session_state.lanc_secao = "💼 Saldo"
    if "import_processado" not in st.session_state:
        st.session_state.import_processado = False
    if "import_dados" not in st.session_state:
        st.session_state.import_dados = {}

    # Sidebar - Seleção de seção
    secao = st.sidebar.radio(
        "Escolha a seção:",
        ["💼 Saldo", "➕ Novo Lançamento", "📥 Importar Excel", "✅ Validação"],
        index=["💼 Saldo", "➕ Novo Lançamento", "📥 Importar Excel", "✅ Validação"].index(st.session_state.lanc_secao)
    )
    st.session_state.lanc_secao = secao

    # ==========================================================
    # SEÇÃO 1 - SALDO
    # ==========================================================
    if secao == "💼 Saldo":
        st.subheader("💼 Lançar Saldo Inicial / Final")
        col1, col2, col3 = st.columns(3)
        with col1:
            data_ref = st.date_input("Data de Referência", value=date.today(), key="saldo_data")
        with col2:
            tipo_saldo = st.selectbox("Tipo", ["Inicial", "Final"], key="saldo_tipo")
        with col3:
            valor_saldo = st.number_input("Valor (R$)", format="%.2f", min_value=0.0, key="saldo_valor")
        obs_saldo = st.text_area("Observação (opcional)", height=80, key="saldo_obs")
        if st.button("💾 Salvar Saldo", type="primary", use_container_width=True):
            data_ref_db = pd.to_datetime(data_ref).date().isoformat()
            with engine.begin() as conn:
                conn.execute(
                    text("DELETE FROM saldos WHERE data_referencia = :d AND tipo = :t"),
                    {"d": data_ref_db, "t": tipo_saldo},
                )
                conn.execute(
                    text("INSERT INTO saldos (data_referencia, tipo, valor, observacao) VALUES (:d, :t, :v, :o)"),
                    {"d": data_ref_db, "t": tipo_saldo, "v": float(valor_saldo), "o": obs_saldo},
                )
            st.success("✅ Saldo salvo!")
            st.rerun()
        st.divider()
        st.subheader("📊 Histórico de Saldos")
        df_saldos = pd.read_sql("SELECT id, data_referencia, tipo, valor FROM saldos ORDER BY data_referencia DESC LIMIT 50", engine)
        if not df_saldos.empty:
            df_saldos["valor"] = df_saldos["valor"].apply(fmt_brl)
            st.dataframe(df_saldos, use_container_width=True)
        else:
            st.info("Nenhum saldo registrado.")

    # ==========================================================
    # SEÇÃO 2 - NOVO LANÇAMENTO
    # ==========================================================
    elif secao == "➕ Novo Lançamento":
        st.subheader("Novo Lançamento")

        df_plano = pd.read_sql("SELECT * FROM plano_contas WHERE aceita_lancamento = 1", engine)

        if df_plano.empty:
            st.warning("Nenhuma conta disponível para lançamento (verifique aceita_lancamento).")
            st.stop()

        conta = st.selectbox("Selecione a Conta", df_plano["descricao"], key="lanc_conta")
        conta_row = df_plano[df_plano["descricao"] == conta].iloc[0]
        conta_codigo = str(conta_row["codigo"])
        natureza_sel = str(conta_row["natureza"]).strip()

        st.caption("📌 Aqui você informa o **vencimento** em 'Data de Pagamento'.")

        data_competencia = st.date_input("Data de Competência", value=date.today(), key="lanc_comp_date")
        data_vencimento = st.date_input("Vencimento (Data de Pagamento)", value=date.today(), key="lanc_venc_date")
        valor = st.number_input("Valor", min_value=0.0, format="%.2f", key="lanc_valor")

        status_opcoes = status_opcoes_por_natureza(natureza_sel)
        status_padrao = default_status_por_natureza(natureza_sel)
        status = st.selectbox(
            "Status",
            status_opcoes,
            index=status_opcoes.index(status_padrao) if status_padrao in status_opcoes else 0,
            key="lanc_status"
        )

        centro_custo = st.text_input("Centro de Custo", key="lanc_cc")
        unidade = st.text_input("Unidade", key="lanc_unidade")
        projeto = st.text_input("Projeto", key="lanc_projeto")
        observacao = st.text_area("Observação", key="lanc_obs")

        if st.button("Salvar Lançamento", type="primary", use_container_width=True):
            data_comp_db = pd.to_datetime(data_competencia).date().isoformat()
            data_venc_db = pd.to_datetime(data_vencimento).date().isoformat()

            with engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        INSERT INTO lancamentos (
                            data_competencia, data_pagamento, valor, status,
                            plano_conta_id, centro_custo, unidade, projeto, observacao
                        ) VALUES (:dc, :dp, :v, :s, :pc, :cc, :u, :p, :o)
                    """
                    ),
                    {
                        "dc": data_comp_db,
                        "dp": data_venc_db,
                        "v": float(valor),
                        "s": status,
                        "pc": conta_codigo,
                        "cc": centro_custo,
                        "u": unidade,
                        "p": projeto,
                        "o": observacao,
                    },
                )

            st.success("✅ Lançamento criado com sucesso!")
            st.rerun()

    # ==========================================================
    # SEÇÃO 3 - IMPORTAÇÃO EXCEL
    # ==========================================================
    elif secao == "📥 Importar Excel":
        st.subheader("📥 Importar lançamentos do Excel")

        st.markdown(
            "<div class='smallmuted'>Fluxo: 1) Enviar → 2) Processar → 3) Corrigir → 4) Inserir</div>",
            unsafe_allow_html=True,
        )

        # Upload
        arquivo = st.file_uploader("Envie o Excel", type=["xlsx"], key="import_file")
        
        colp1, colp2 = st.columns([1, 1])
        with colp1:
            processar = st.button("⚙️ Processar arquivo", type="primary", key="import_btn_processar")
        with colp2:
            importar_conciliado = st.checkbox("Importar como Conciliado", value=False, key="import_conciliado")

        # Processar arquivo
        if processar and arquivo:
            try:
                df_base = pd.read_excel(arquivo)
                
                st.write("Preview:")
                st.dataframe(df_base.head(10), use_container_width=True)

                colunas_obrigatorias = ["Competência", "Data Pagamento", "Valor (R$)", "Codigo Natureza"]
                faltando = [c for c in colunas_obrigatorias if c not in df_base.columns]
                if faltando:
                    st.error(f"Colunas faltando: {faltando}")
                    st.stop()

                # Normalizar dados
                df_base["Competência"] = pd.to_datetime(df_base["Competência"], errors="coerce")
                df_base["Data Pagamento"] = pd.to_datetime(df_base["Data Pagamento"], errors="coerce")
                df_base["Valor (R$)"] = pd.to_numeric(df_base["Valor (R$)"], errors="coerce")
                df_base["Codigo Natureza"] = df_base["Codigo Natureza"].astype(str).str.strip()
                
                if "Plano de Natureza Financeira" in df_base.columns:
                    df_base["Plano de Natureza Financeira"] = df_base["Plano de Natureza Financeira"].astype(str).str.strip()

                df_base = df_base.dropna(subset=["Competência", "Data Pagamento", "Valor (R$)"])

                # Plano de contas
                df_planos = pd.read_sql("SELECT codigo, natureza, descricao FROM plano_contas", engine)
                df_planos["codigo"] = df_planos["codigo"].astype(str).str.strip()
                df_merge = df_base.merge(df_planos, left_on="Codigo Natureza", right_on="codigo", how="left")

                # Separar
                sem_plano = df_merge[df_merge["natureza"].isna()].copy()
                com_plano = df_merge[df_merge["natureza"].notna()].copy()

                divergencias = []
                validos = []

                for idx, row in com_plano.iterrows():
                    desc_arquivo = str(row.get("Plano de Natureza Financeira", "")).strip()
                    desc_plano = str(row["descricao"]).strip()
                    
                    if desc_arquivo and desc_arquivo != desc_plano:
                        divergencias.append(row)
                    else:
                        validos.append(row)

                # Armazenar no session_state
                st.session_state.import_processado = True
                st.session_state.import_dados = {
                    "sem_plano": sem_plano,
                    "divergencias": divergencias,
                    "validos": validos,
                    "importar_conciliado": importar_conciliado,
                    "df_base": df_base,
                    "df_planos": df_planos
                }
                
                st.success("✅ Arquivo processado!")
                st.rerun()

            except Exception as e:
                st.error(f"Erro: {e}")

        # Exibir resultados se já foi processado
        if st.session_state.import_processado and st.session_state.import_dados:
            dados = st.session_state.import_dados
            sem_plano = dados.get("sem_plano", pd.DataFrame())
            divergencias = dados.get("divergencias", [])
            validos = dados.get("validos", [])
            importar_conciliado = dados.get("importar_conciliado", False)

            # Erros - Sem Plano
            if not sem_plano.empty:
                st.subheader("⚠️ Códigos sem Plano de Contas")
                df_sem_display = sem_plano[["Codigo Natureza", "Valor (R$)", "Plano de Natureza Financeira"]].copy()
                df_sem_display.columns = ["Código Inválido", "Valor", "Descrição"]
                st.dataframe(df_sem_display, use_container_width=True)

                st.divider()
                st.subheader("✏️ Corrigir Códigos")

                df_plano_full = pd.read_sql("SELECT codigo, descricao FROM plano_contas WHERE aceita_lancamento = 1", engine)
                opcoes_codigos = df_plano_full["codigo"].astype(str).str.strip().tolist()
                opcoes_display = {row['codigo']: f"{row['codigo']} - {row['descricao']}" for _, row in df_plano_full.iterrows()}

                if "corr_sem_plano" not in st.session_state:
                    st.session_state.corr_sem_plano = {}

                for idx, (i, row) in enumerate(sem_plano.iterrows()):
                    col1, col2, col3 = st.columns([2, 3, 1])

                    with col1:
                        st.write(f"**Linha {idx + 1}**")
                        st.write(f"❌ Código inválido: `{row['Codigo Natureza']}`")
                        st.write(f"📄 Descrição atual: **{row.get('Plano de Natureza Financeira', 'N/A')}**")
                        st.write(f"💰 Valor: **{fmt_brl(row['Valor (R$)'])}**")

                    with col2:
                        novo_cod = st.selectbox(
                            "Selecione código correto",
                            opcoes_codigos,
                            format_func=lambda x: opcoes_display.get(x, x),
                            key=f"corr_sem_{i}"
                        )
                        st.session_state.corr_sem_plano[i] = novo_cod

                    with col3:
                        st.write("")

            else:
                st.success("✅ Todos os códigos existem!")

            # Erros - Divergências
            if divergencias:
                st.subheader("⚠️ Divergências: Descrição")
                df_div = pd.DataFrame(divergencias)
                df_div_display = df_div[["Codigo Natureza", "Valor (R$)", "Plano de Natureza Financeira", "descricao"]].copy()
                df_div_display.columns = ["Código", "Valor", "Descrição Arquivo", "Descrição Plano"]
                st.dataframe(df_div_display, use_container_width=True)

                st.divider()
                st.subheader("✏️ Corrigir Divergências")

                if "corr_divergencias" not in st.session_state:
                    st.session_state.corr_divergencias = {}

                for idx, (i, row) in enumerate(df_div.iterrows()):
                    st.markdown(f"**Linha {idx + 1}** | Código: `{row['Codigo Natureza']}` | Valor: **{fmt_brl(row['Valor (R$)'])}**")
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        st.write("**❌ ATUALMENTE (Arquivo):**")
                        st.markdown(f"```\n{row.get('Plano de Natureza Financeira', '')}\n```")

                    with col2:
                        st.write("**✅ VAI FICAR (Plano de Contas):**")
                        st.markdown(f"```\n{row['descricao']}\n```")

                    usar_plano = st.radio(
                        "Usar",
                        ["Plano", "Arquivo"],
                        key=f"radio_div_{i}",
                        horizontal=True
                    )
                    st.session_state.corr_divergencias[i] = usar_plano
                    st.divider()

            else:
                st.success("✅ Sem divergências!")

            # Inserir
            st.divider()
            if st.button("💾 Inserir no banco", type="primary", use_container_width=True):
                df_insert_list = []

                # Válidos
                df_insert_list.extend(validos)

                # Corrigidos sem plano
                for i, novo_cod in st.session_state.get("corr_sem_plano", {}).items():
                    row = sem_plano.loc[i].copy()
                    row["Codigo Natureza"] = novo_cod
                    df_insert_list.append(row)

                # Corrigidos divergências
                for i, opcao in st.session_state.get("corr_divergencias", {}).items():
                    if i < len(divergencias):
                        df_insert_list.append(divergencias[i])

                if df_insert_list:
                    df_insert = pd.DataFrame(df_insert_list)
                    df_insert["Codigo Natureza"] = df_insert["Codigo Natureza"].astype(str).str.strip()
                    
                    df_planos_verify = pd.read_sql("SELECT codigo, natureza FROM plano_contas", engine)
                    df_planos_verify["codigo"] = df_planos_verify["codigo"].astype(str).str.strip()
                    
                    df_insert = df_insert.merge(df_planos_verify, left_on="Codigo Natureza", right_on="codigo", how="left", suffixes=("", "_new"))
                    df_insert["natureza"] = df_insert["natureza_new"].fillna(df_insert["natureza"])

                    df_final = pd.DataFrame()
                    df_final["data_competencia"] = df_insert["Competência"].dt.date.astype(str)
                    df_final["data_pagamento"] = df_insert["Data Pagamento"].dt.date.astype(str)
                    df_final["valor"] = df_insert["Valor (R$)"].astype(float)
                    df_final["plano_conta_id"] = df_insert["Codigo Natureza"].astype(str).str.strip()

                    if importar_conciliado:
                        df_final["status"] = df_insert["natureza"].apply(
                            lambda x: "Recebido" if str(x).strip() == "Entrada" else "Pago"
                        )
                    else:
                        df_final["status"] = df_insert["natureza"].apply(
                            lambda x: "A receber" if str(x).strip() == "Entrada" else "A pagar"
                        )

                    df_final["centro_custo"] = df_insert.get("Centro de Custo", "")
                    df_final["unidade"] = df_insert.get("Empresa", "")
                    df_final["projeto"] = df_insert.get("Código Contrato", "")
                    df_final["observacao"] = ""

                    try:
                        with engine.begin() as conn:
                            df_final.to_sql("lancamentos", con=conn, if_exists="append", index=False, chunksize=100)
                        st.success(f"✅ Inseridos {len(df_final)} lançamento(s)!")
                        st.session_state.import_processado = False
                        st.session_state.import_dados = {}
                        st.session_state.corr_sem_plano = {}
                        st.session_state.corr_divergencias = {}
                        st.rerun()
                    except Exception as e:
                        st.error(f"Erro: {e}")

                # ==========================================================
    # SEÇÃO 4 - VALIDAÇÃO
    # ==========================================================
    elif secao == "✅ Validação":
        st.subheader("✅ Validação / Conciliação de Lançamentos")
        
        # Filtros
        st.caption("Filtros")
        colf1, colf2, colf3 = st.columns(3)

        with colf1:
            data_inicio = st.date_input("Data Início", value=date(date.today().year, 1, 1), key="val_data_inicio")
        with colf2:
            data_fim = st.date_input("Data Fim", value=date.today(), key="val_data_fim")
        with colf3:
            st.write("")

        colf4, colf5, colf6, colf7 = st.columns(4)

        with colf4:
            filtro_status = st.multiselect(
                "Status",
                ["A pagar", "A receber", "Atrasado", "Renegociado", "Pago", "Recebido"],
                default=["A pagar", "A receber", "Atrasado", "Renegociado"],
                key="val_status_filter"
            )
        with colf5:
            filtro_unidade = st.text_input("Unidade (contém)", "", key="val_unidade_filter")
        with colf6:
            filtro_cc = st.text_input("Centro de custo (contém)", "", key="val_cc_filter")
        with colf7:
            filtro_projeto = st.text_input("Projeto (contém)", "", key="val_projeto_filter")

        # Buscar lançamentos - SIMPLES SEM ALIAS
        query = """
            SELECT
                l.rowid,
                l.data_competencia,
                l.data_pagamento,
                l.valor,
                l.status,
                l.plano_conta_id,
                p.descricao AS conta_descricao,
                p.natureza,
                l.unidade,
                l.centro_custo,
                l.projeto,
                l.observacao
            FROM lancamentos l
            LEFT JOIN plano_contas p ON l.plano_conta_id = p.codigo
            ORDER BY l.data_pagamento DESC, l.rowid DESC
        """
        
        try:
            df_val = pd.read_sql(query, engine)
            df_val.rename(columns={"rowid": "id"}, inplace=True)
        except Exception as e:
            st.error(f"Erro ao buscar lançamentos: {e}")
            df_val = pd.DataFrame()

        if df_val.empty:
            st.warning("⚠️ Nenhum lançamento encontrado no banco de dados.")
            st.info("Importe lançamentos usando a aba '📥 Importar Excel' ou crie um novo lançamento.")
        else:
            st.success(f"✅ {len(df_val)} lançamento(s) encontrado(s) no banco")
            
            # Remover nulos de id
            df_val = df_val.dropna(subset=["id"])

            if df_val.empty:
                st.error("Nenhum lançamento com ID válido.")
            else:
                # Converter datas para datetime
                df_val["data_pagamento"] = pd.to_datetime(df_val["data_pagamento"], errors="coerce")
                
                # Aplicar filtro de período
                data_inicio_dt = pd.to_datetime(data_inicio)
                data_fim_dt = pd.to_datetime(data_fim)
                df_val = df_val[
                    (df_val["data_pagamento"] >= data_inicio_dt) & 
                    (df_val["data_pagamento"] <= data_fim_dt)
                ]

                if df_val.empty:
                    st.info(f"Nenhum lançamento no período de {data_inicio} a {data_fim}.")
                else:
                    # Aplicar filtros
                    if filtro_status:
                        df_val = df_val[df_val["status"].isin(filtro_status)]
                    if filtro_unidade.strip():
                        df_val = df_val[df_val["unidade"].fillna("").str.contains(filtro_unidade.strip(), case=False)]
                    if filtro_cc.strip():
                        df_val = df_val[df_val["centro_custo"].fillna("").str.contains(filtro_cc.strip(), case=False)]
                    if filtro_projeto.strip():
                        df_val = df_val[df_val["projeto"].fillna("").str.contains(filtro_projeto.strip(), case=False)]

                    if df_val.empty:
                        st.info("Nenhum lançamento com esses filtros.")
                    else:
                        st.divider()
                        st.write(f"**Total de registros filtrados: {len(df_val)}**")
                        st.divider()
                        
                        # Opção 1: Atualizar TODOS para um status
                        st.subheader("🔄 Atualizar Status em Lote")
                        col_lote1, col_lote2, col_lote3 = st.columns([2, 2, 1])
                        
                        with col_lote1:
                            status_novo_lote = st.selectbox(
                                "Novo status para TODOS",
                                ["A pagar", "A receber", "Atrasado", "Renegociado", "Pago", "Recebido"],
                                key="val_status_lote"
                            )
                        
                        with col_lote2:
                            data_lote = st.date_input("Data real (se Pago/Recebido)", value=date.today(), key="val_data_lote")
                        
                        with col_lote3:
                            st.write("")
                            if st.button("✅ Aplicar a Todos", key="val_btn_lote", use_container_width=True):
                                data_lote_db = pd.to_datetime(data_lote).date().isoformat()
                                ids_para_atualizar = df_val["id"].astype(int).tolist()
                                
                                with engine.begin() as conn:
                                    for _id in ids_para_atualizar:
                                        conn.execute(
                                            text("UPDATE lancamentos SET status = :s WHERE rowid = :id"),
                                            {"s": status_novo_lote, "id": _id}
                                        )
                                        
                                        if status_novo_lote in ("Pago", "Recebido"):
                                            conn.execute(
                                                text("UPDATE lancamentos SET data_pagamento = :d WHERE rowid = :id"),
                                                {"d": data_lote_db, "id": _id}
                                            )
                                
                                st.success(f"✅ Status atualizado para {len(ids_para_atualizar)} lançamento(s)!")
                                st.rerun()

                        st.divider()

                        # Opção 2: Atualizar UM A UM
                        st.subheader("✏️ Ajustar Um a Um")

                        df_val["vencimento_dt"] = pd.to_datetime(df_val["data_pagamento"], errors="coerce")
                        df_val["Dias em atraso"] = (pd.Timestamp(date.today()) - df_val["vencimento_dt"]).dt.days
                        df_val.loc[df_val["Dias em atraso"] < 0, "Dias em atraso"] = 0
                        df_val["Valor (R$)"] = df_val["valor"].apply(fmt_brl)

                        if "val_edits" not in st.session_state:
                            st.session_state.val_edits = {}

                        for idx, (i, row) in enumerate(df_val.iterrows()):
                            col1, col2, col3, col4 = st.columns([1, 2, 2, 1])

                            with col1:
                                st.write(f"**ID {int(row['id'])}**")
                                st.write(f"{row['data_pagamento'].strftime('%Y-%m-%d')}")
                                st.write(f"{row['Valor (R$)']}")

                            with col2:
                                conta = row.get('conta_descricao', 'N/A')
                                if pd.isna(conta):
                                    conta = "SEM PLANO"
                                st.write(f"**{str(conta)[:30]}**")
                                st.write(f"Status: `{row['status']}`")

                            with col3:
                                novo_status = st.selectbox(
                                    "Novo status",
                                    ["A pagar", "A receber", "Atrasado", "Renegociado", "Pago", "Recebido"],
                                    index=["A pagar", "A receber", "Atrasado", "Renegociado", "Pago", "Recebido"].index(row['status']) if row['status'] in ["A pagar", "A receber", "Atrasado", "Renegociado", "Pago", "Recebido"] else 0,
                                    key=f"val_status_{int(row['id'])}"
                                )
                                
                                data_real = st.date_input(
                                    "Data real",
                                    value=date.today(),
                                    key=f"val_data_{int(row['id'])}"
                                )
                                
                                st.session_state.val_edits[int(row['id'])] = {
                                    "status": novo_status,
                                    "data": data_real
                                }

                            with col4:
                                st.write("")
                                if st.button("✅ Salvar", key=f"val_btn_{int(row['id'])}", use_container_width=True):
                                    _id = int(row['id'])
                                    edit = st.session_state.val_edits.get(_id)
                                    
                                    if edit:
                                        data_db = pd.to_datetime(edit["data"]).date().isoformat()
                                        
                                        with engine.begin() as conn:
                                            conn.execute(
                                                text("UPDATE lancamentos SET status = :s WHERE rowid = :id"),
                                                {"s": edit["status"], "id": _id}
                                            )
                                            
                                            if edit["status"] in ("Pago", "Recebido"):
                                                conn.execute(
                                                    text("UPDATE lancamentos SET data_pagamento = :d WHERE rowid = :id"),
                                                    {"d": data_db, "id": _id}
                                                )
                                        
                                        st.success(f"✅ Lançamento {_id} atualizado!")
                                        st.rerun()

                            st.divider()

elif menu == "DFC - Caixa":
    st.header("💰 DFC - Regime de Caixa (Hierárquico + Config Oficial)")

    if "plano_contas" not in inspector.get_table_names():
        st.error("Importe o Plano de Contas primeiro.")
        st.stop()

    # ler configs oficiais da auditoria
    modelo_oficial = get_config("modelo_dfc_oficial", "Saldo + Entradas - Saídas")
    considerar_somente_conciliados = get_config_bool("considerar_somente_conciliados", True)
    usar_saldo_lancado = get_config_bool("usar_saldo_lancado", True)
    usar_saldo_calculado = get_config_bool("usar_saldo_calculado", False)
    forcar_saida_negativa = get_config_bool("forcar_saida_negativa", True)

    st.caption(f"📌 Modelo Oficial Ativo: {modelo_oficial}")

    col1, col2 = st.columns(2)
    with col1:
        data_inicio = st.date_input("Data Inicial", value=date.today().replace(day=1))
    with col2:
        data_fim = st.date_input("Data Final", value=date.today())

    data_inicio_dt = pd.to_datetime(data_inicio)
    data_fim_dt = pd.to_datetime(data_fim)
    data_inicio_db = pd.to_datetime(data_inicio).date().isoformat()
    data_fim_db = pd.to_datetime(data_fim).date().isoformat()

    # Movimentações (com ou sem conciliação conforme configuração)
    df_mov = pd.read_sql(
        """
        SELECT
            l.data_pagamento,
            l.valor,
            l.status,
            p.codigo,
            p.descricao,
            p.natureza
        FROM lancamentos l
        JOIN plano_contas p
          ON l.plano_conta_id = p.codigo
    """,
        engine,
    )

    if df_mov.empty:
        st.warning("Nenhum lançamento encontrado.")
        st.stop()

    df_mov["data_pagamento"] = pd.to_datetime(df_mov["data_pagamento"], errors="coerce", format="mixed")
    df_mov = df_mov.dropna(subset=["data_pagamento"])

    if considerar_somente_conciliados:
        df_mov = df_mov[
            ((df_mov["natureza"] == "Entrada") & (df_mov["status"] == "Recebido"))
            | ((df_mov["natureza"] == "Saída") & (df_mov["status"] == "Pago"))
        ]

    if df_mov.empty:
        st.warning("Nenhum lançamento após aplicar filtros (conciliação).")
        st.stop()

    df_periodo = df_mov[(df_mov["data_pagamento"] >= data_inicio_dt) & (df_mov["data_pagamento"] <= data_fim_dt)].copy()

    if df_periodo.empty:
        st.warning("Sem movimentação no período selecionado.")
        st.stop()

    # Saldo inicial lançado (<= data_inicio)
    saldo_inicial_lancado = None
    data_saldo_ini = None
    if usar_saldo_lancado:
        df_saldo_ini = pd.read_sql(
            """
            SELECT valor, data_referencia
            FROM saldos
            WHERE tipo = 'Inicial'
              AND data_referencia <= :d
            ORDER BY data_referencia DESC, id DESC
            LIMIT 1
        """,
            engine,
            params={"d": data_inicio_db},
        )
        if not df_saldo_ini.empty:
            saldo_inicial_lancado = float(df_saldo_ini["valor"].iloc[0])
            data_saldo_ini = df_saldo_ini["data_referencia"].iloc[0]

    # Saldo inicial calculado (histórico antes do período)
    saldo_inicial_calc = 0.0
    if usar_saldo_calculado:
        df_antes = df_mov[df_mov["data_pagamento"] < data_inicio_dt].copy()
        df_antes["valor_ajustado"] = df_antes.apply(
            lambda r: r["valor"] if r["natureza"] == "Entrada" else -r["valor"], axis=1
        )
        saldo_inicial_calc = float(df_antes["valor_ajustado"].sum())

    # combinar saldos conforme configuração
    saldo_inicial = 0.0
    if usar_saldo_lancado and saldo_inicial_lancado is not None:
        saldo_inicial += float(saldo_inicial_lancado)
    if usar_saldo_calculado:
        saldo_inicial += float(saldo_inicial_calc)

    # Totais do período
    entradas = df_periodo[df_periodo["natureza"] == "Entrada"].copy()
    saidas = df_periodo[df_periodo["natureza"] == "Saída"].copy()

    total_entradas = float(entradas["valor"].sum())
    total_saidas_raw = float(saidas["valor"].sum())
    total_saidas = -abs(total_saidas_raw) if forcar_saida_negativa else -float(total_saidas_raw)

    # Aplicar modelo oficial
    if modelo_oficial == "Saldo + Entradas + Saídas":
        saldo_final_calc = saldo_inicial + total_entradas + total_saidas
    elif modelo_oficial == "Saldo + Entradas - Saídas":
        saldo_final_calc = saldo_inicial + total_entradas - total_saidas
    elif modelo_oficial == "Saldo + (Entradas - Saídas)":
        saldo_final_calc = saldo_inicial + (total_entradas - total_saidas)
    elif modelo_oficial == "Saldo + Somente Entradas":
        saldo_final_calc = saldo_inicial + total_entradas
    elif modelo_oficial == "Saldo + Somente Saídas":
        saldo_final_calc = saldo_inicial + total_saidas
    else:
        saldo_final_calc = saldo_inicial + total_entradas + total_saidas

    # Saldo final lançado (opcional)
    saldo_final_lancado = None
    data_saldo_fim = None
    df_saldo_fim = pd.read_sql(
        """
        SELECT valor, data_referencia
        FROM saldos
        WHERE tipo = 'Final'
          AND data_referencia <= :d
        ORDER BY data_referencia DESC, id DESC
        LIMIT 1
    """,
        engine,
        params={"d": data_fim_db},
    )
    if not df_saldo_fim.empty:
        saldo_final_lancado = float(df_saldo_fim["valor"].iloc[0])
        data_saldo_fim = df_saldo_fim["data_referencia"].iloc[0]

    # =========================
    # HIERARQUIA + ROLL-UP
    # =========================
    df_plano = pd.read_sql("SELECT codigo, descricao, natureza FROM plano_contas", engine)
    df_plano["codigo"] = df_plano["codigo"].astype(str).str.strip()

    df_sum = df_periodo.groupby(["codigo", "natureza"], as_index=False)["valor"].sum()
    soma_por_codigo = {(r["codigo"], r["natureza"]): float(r["valor"]) for _, r in df_sum.iterrows()}

    plano_desc = {row["codigo"]: str(row["descricao"]) for _, row in df_plano.iterrows()}
    plano_nat = {row["codigo"]: str(row["natureza"]) for _, row in df_plano.iterrows()}

    filhos = {cod: [] for cod in plano_desc.keys()}

    for cod in plano_desc.keys():
        pais = codigo_pais(cod)
        pai_existente = None
        for p in pais:
            if p in plano_desc:
                pai_existente = p
                break
        if pai_existente:
            filhos[pai_existente].append(cod)

    for k in filhos:
        filhos[k] = sorted(list(set(filhos[k])))

    def tem_pai(cod: str) -> bool:
        for p in codigo_pais(cod):
            if p in plano_desc:
                return True
        return False

    roots_entrada = sorted([c for c in plano_desc.keys() if not tem_pai(c) and plano_nat.get(c) == "Entrada"])
    roots_saida = sorted([c for c in plano_desc.keys() if not tem_pai(c) and plano_nat.get(c) == "Saída"])

    cache_total = {}

    def total_no(cod: str, natureza: str) -> float:
        key = (cod, natureza)
        if key in cache_total:
            return cache_total[key]
        base = soma_por_codigo.get((cod, natureza), 0.0)
        for ch in filhos.get(cod, []):
            nat_ch = plano_nat.get(ch, natureza)
            if nat_ch != natureza:
                continue
            base += total_no(ch, natureza)
        cache_total[key] = float(base)
        return float(base)

    def render_no(cod: str, natureza: str, depth: int = 0):
        desc = plano_desc.get(cod, cod)
        total = total_no(cod, natureza)
        tem_filhos = len([c for c in filhos.get(cod, []) if plano_nat.get(c) == natureza]) > 0

        label = f"{cod} - {desc}  "
        valor_txt = fmt_brl(total if natureza == "Entrada" else -total)

        if tem_filhos:
            with st.expander(f"{label} | {valor_txt}", expanded=(depth == 0)):
                st.markdown(
                    f"<div class='linha'><div class='desc'><b>Total do grupo</b></div><div class='valor'><b>{valor_txt}</b></div></div>",
                    unsafe_allow_html=True,
                )
                for ch in filhos.get(cod, []):
                    if plano_nat.get(ch) != natureza:
                        continue
                    render_no(ch, natureza, depth + 1)
        else:
            st.markdown(
                f"<div class='linha'><div class='desc'>{label}</div><div class='valor'>{valor_txt}</div></div>",
                unsafe_allow_html=True,
            )

    # =========================
    # PAINEL RESUMO
    # =========================
    st.markdown("<div class='titulo-bloco'>Resumo do Período</div>", unsafe_allow_html=True)
    colr1, colr2, colr3, colr4 = st.columns(4)
    colr1.metric("Saldo inicial", fmt_brl(saldo_inicial))
    colr2.metric("Entradas", fmt_brl(total_entradas))
    colr3.metric("Saídas", fmt_brl(total_saidas))
    colr4.metric("Saldo final (calculado)", fmt_brl(saldo_final_calc))

    # Geração de caixa SEMPRE consistente com saldo final exibido
    geracao_caixa = saldo_final_calc - saldo_inicial

    if usar_saldo_lancado and saldo_inicial_lancado is not None:
        st.caption(f"Saldo inicial **lançado** usado: {fmt_brl(saldo_inicial_lancado)} (ref.: {data_saldo_ini})")
    if usar_saldo_calculado:
        st.caption(f"Saldo inicial **calculado** (histórico): {fmt_brl(saldo_inicial_calc)}")
    if saldo_final_lancado is not None:
        diff = saldo_final_calc - saldo_final_lancado
        st.caption(
            f"Saldo final **lançado**: {fmt_brl(saldo_final_lancado)} (ref.: {data_saldo_fim}) | Diferença (Calc - Lançado): {fmt_brl(diff)}"
        )

    st.divider()

    # =========================
    # DFC HIERÁRQUICO
    # =========================
    st.markdown("<div class='titulo-bloco'>ATIVIDADES OPERACIONAIS</div>", unsafe_allow_html=True)

    cA, cB = st.columns(2)
    with cA:
        st.markdown(
            "<div class='titulo-bloco'>RECEBIMENTOS <span class='badge'>no período</span></div>",
            unsafe_allow_html=True,
        )
        if not roots_entrada:
            st.info("Nenhuma raiz de Entrada encontrada no plano de contas.")
        else:
            for r in roots_entrada:
                render_no(r, "Entrada", depth=0)

    with cB:
        st.markdown(
            "<div class='titulo-bloco'>PAGAMENTOS <span class='badge'>no período</span></div>",
            unsafe_allow_html=True,
        )
        if not roots_saida:
            st.info("Nenhuma raiz de Saída encontrada no plano de contas.")
        else:
            for r in roots_saida:
                render_no(r, "Saída", depth=0)

    st.divider()
    st.markdown("<div class='titulo-bloco'>Geração de Caixa no Período</div>", unsafe_allow_html=True)
    st.markdown(
        f"<div class='linha'><div class='desc'><b>Saldo final - Saldo inicial</b></div><div class='valor'><b>{fmt_brl(geracao_caixa)}</b></div></div>",
        unsafe_allow_html=True,
    )

    st.divider()
    
    if st.checkbox("📋 Ver detalhamento de lançamentos", value=False):
        st.subheader("Lançamentos do período")
        df_detalhes = df_periodo[["data_pagamento", "codigo", "descricao", "valor", "status", "natureza"]].copy()
        df_detalhes = df_detalhes.sort_values("data_pagamento")
        df_detalhes["data_pagamento"] = df_detalhes["data_pagamento"].dt.strftime("%d/%m/%Y")
        df_detalhes["valor"] = df_detalhes["valor"].apply(fmt_brl)
        df_detalhes.columns = ["Data", "Código", "Descrição", "Valor", "Status", "Natureza"]
        st.dataframe(df_detalhes, use_container_width=True, hide_index=True)

elif menu == "Auditoria DFC":
    st.header("🔎 Auditoria e Motor Oficial da DFC")

    st.subheader("Configuração Base (o que você salvar aqui vira oficial e o DFC obedece)")

    usar_saldo_lancado = st.checkbox(
        "Usar saldo inicial lançado (tabela saldos)",
        value=get_config_bool("usar_saldo_lancado", True),
    )
    usar_saldo_calculado = st.checkbox(
        "Usar saldo inicial calculado pelo histórico",
        value=get_config_bool("usar_saldo_calculado", False),
    )
    considerar_somente_conciliados = st.checkbox(
        "Considerar somente Pago/Recebido",
        value=get_config_bool("considerar_somente_conciliados", True),
    )
    forcar_saida_negativa = st.checkbox(
        "Forçar saídas como valor negativo",
        value=get_config_bool("forcar_saida_negativa", True),
    )

    st.divider()

    st.subheader("Modelo Matemático da DFC")
    st.caption("Escolha como será apurado o saldo final a partir do saldo inicial e movimentos")
    
    modelo_formula = st.selectbox(
        "Fórmula do Saldo Final:",
        [
            "Saldo + Entradas - Saídas",
            "Saldo + Entradas + Saídas",
            "Saldo + (Entradas - Saídas)",
            "Saldo + Somente Entradas",
            "Saldo + Somente Saídas",
        ],
        index=[
            "Saldo + Entradas - Saídas",
            "Saldo + Entradas + Saídas",
            "Saldo + (Entradas - Saídas)",
            "Saldo + Somente Entradas",
            "Saldo + Somente Saídas",
        ].index(get_config("modelo_dfc_oficial", "Saldo + Entradas - Saídas")),
    )

    st.info(f"📌 **Fórmula ativa:** `{modelo_formula}`")

    st.divider()

    st.subheader("🧪 Simular Cálculo")
    
    col_data1, col_data2 = st.columns(2)
    with col_data1:
        data_sim_ini = st.date_input("Data Inicial (Simulação)", value=date.today().replace(day=1), key="audit_sim_ini")
    with col_data2:
        data_sim_fim = st.date_input("Data Final (Simulação)", value=date.today(), key="audit_sim_fim")
    
    if st.button("🔄 Executar Simulação", type="primary", use_container_width=True):
        data_sim_ini_dt = pd.to_datetime(data_sim_ini)
        data_sim_fim_dt = pd.to_datetime(data_sim_fim)
        data_sim_ini_db = data_sim_ini_dt.date().isoformat()
        data_sim_fim_db = data_sim_fim_dt.date().isoformat()

        df_mov_sim = pd.read_sql(
            """
            SELECT
                l.data_pagamento,
                l.valor,
                l.status,
                p.natureza
            FROM lancamentos l
            JOIN plano_contas p ON l.plano_conta_id = p.codigo
        """,
            engine,
        )

        if df_mov_sim.empty:
            st.warning("❌ Nenhum lançamento para simular.")
        else:
            df_mov_sim["data_pagamento"] = pd.to_datetime(df_mov_sim["data_pagamento"], errors="coerce")
            df_mov_sim = df_mov_sim.dropna(subset=["data_pagamento"])

            # Aplicar filtro de conciliação
            if considerar_somente_conciliados:
                df_mov_sim = df_mov_sim[
                    ((df_mov_sim["natureza"] == "Entrada") & (df_mov_sim["status"] == "Recebido")) |
                    ((df_mov_sim["natureza"] == "Saída") & (df_mov_sim["status"] == "Pago"))
                ]

            if df_mov_sim.empty:
                st.warning("❌ Nenhum lançamento conciliado encontrado.")
            else:
                # Separar entradas e saídas
                df_ent = df_mov_sim[df_mov_sim["natureza"] == "Entrada"]
                df_sai = df_mov_sim[df_mov_sim["natureza"] == "Saída"]
                
                total_entradas_sim = float(df_ent["valor"].sum())
                total_saidas_sim = float(df_sai["valor"].sum())
                
                if forcar_saida_negativa:
                    total_saidas_sim = -abs(total_saidas_sim)
                else:
                    total_saidas_sim = -float(total_saidas_sim)

                # Calcular saldo inicial
                saldo_inicial_sim = 0.0
                if usar_saldo_lancado:
                    df_saldo_sim = pd.read_sql(
                        """
                        SELECT valor
                        FROM saldos
                        WHERE tipo = 'Inicial' AND data_referencia <= :d
                        ORDER BY data_referencia DESC LIMIT 1
                    """,
                        engine,
                        params={"d": data_sim_ini_db},
                    )
                    if not df_saldo_sim.empty:
                        saldo_inicial_sim += float(df_saldo_sim["valor"].iloc[0])

                if usar_saldo_calculado:
                    df_hist_sim = df_mov_sim[df_mov_sim["data_pagamento"] < data_sim_ini_dt].copy()
                    df_hist_sim["valor_ajustado"] = df_hist_sim.apply(
                        lambda r: r["valor"] if r["natureza"] == "Entrada" else -r["valor"],
                        axis=1,
                    )
                    saldo_inicial_sim += float(df_hist_sim["valor_ajustado"].sum())

                # Aplicar período
                df_periodo_sim = df_mov_sim[
                    (df_mov_sim["data_pagamento"] >= data_sim_ini_dt) & 
                    (df_mov_sim["data_pagamento"] <= data_sim_fim_dt)
                ].copy()

                total_entradas_periodo = float(df_periodo_sim[df_periodo_sim["natureza"] == "Entrada"]["valor"].sum())
                total_saidas_periodo = float(df_periodo_sim[df_periodo_sim["natureza"] == "Saída"]["valor"].sum())

                if forcar_saida_negativa:
                    total_saidas_periodo = -abs(total_saidas_periodo)
                else:
                    total_saidas_periodo = -float(total_saidas_periodo)

                # Aplicar fórmula
                if modelo_formula == "Saldo + Entradas - Saídas":
                    saldo_final_sim = saldo_inicial_sim + total_entradas_periodo - total_saidas_periodo
                elif modelo_formula == "Saldo + Entradas + Saídas":
                    saldo_final_sim = saldo_inicial_sim + total_entradas_periodo + total_saidas_periodo
                elif modelo_formula == "Saldo + (Entradas - Saídas)":
                    saldo_final_sim = saldo_inicial_sim + (total_entradas_periodo - total_saidas_periodo)
                elif modelo_formula == "Saldo + Somente Entradas":
                    saldo_final_sim = saldo_inicial_sim + total_entradas_periodo
                elif modelo_formula == "Saldo + Somente Saídas":
                    saldo_final_sim = saldo_inicial_sim + total_saidas_periodo
                else:
                    saldo_final_sim = saldo_inicial_sim + total_entradas_periodo - total_saidas_periodo

                st.success("✅ Simulação Executada!")
                st.markdown("<div class='titulo-bloco'>Resultado da Simulação</div>", unsafe_allow_html=True)
                
                col_s1, col_s2, col_s3, col_s4 = st.columns(4)
                col_s1.metric("Saldo Inicial", fmt_brl(saldo_inicial_sim))
                col_s2.metric("Entradas", fmt_brl(total_entradas_periodo))
                col_s3.metric("Saídas", fmt_brl(total_saidas_periodo))
                col_s4.metric("🎯 Saldo Final", fmt_brl(saldo_final_sim))

                st.divider()
                st.subheader("📋 Top 20 Movimentações")
                df_top = df_periodo_sim.nlargest(20, "valor")[["data_pagamento", "valor", "status", "natureza"]]
                if not df_top.empty:
                    df_top = df_top.copy()
                    df_top["data_pagamento"] = df_top["data_pagamento"].dt.strftime("%d/%m/%Y")
                    df_top["valor"] = df_top["valor"].apply(fmt_brl)
                    df_top.columns = ["Data", "Valor", "Status", "Natureza"]
                    st.dataframe(df_top, use_container_width=True, hide_index=True)

    st.divider()
    st.subheader("💾 Salvar como Configuração Oficial")
    st.warning("⚠️ Os valores salvos aqui serão usados **automaticamente** pelo DFC - Caixa")

    if st.button("🔐 Salvar Configuração Oficial", type="primary", use_container_width=True):
        set_config("modelo_dfc_oficial", modelo_formula)
        set_config("usar_saldo_lancado", str(usar_saldo_lancado))
        set_config("usar_saldo_calculado", str(usar_saldo_calculado))
        set_config("considerar_somente_conciliados", str(considerar_somente_conciliados))
        set_config("forcar_saida_negativa", str(forcar_saida_negativa))
        st.success("✅ Configuração salva com sucesso! O DFC agora usará esses parâmetros.")
        st.balloons()

elif menu == "DRE - Competência":
    st.header("📈 DRE - Regime de Competência")
    st.info("Em construção")

elif menu == "Admin":
    st.header("🛠️ Admin - Gerenciar Dados")
    st.warning("⚠️ Use com cuidado!")
    st.info("Em construção")