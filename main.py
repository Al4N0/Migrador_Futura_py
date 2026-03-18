import os
import datetime
import customtkinter as ctk
from dotenv import load_dotenv, set_key
from tkinter import messagebox
import threading

# Importar nossas classes de banco de dados
from core import ConexaoFirebird, ConexaoMySQL
from migrador_clientes import MigradorClientes
from migrador_vendas import MigradorVendas
from migrador_itens import MigradorItens

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()


# =============================================================================
# CORES DO TEMA (inspirado no Delphi)
# =============================================================================
COR_FUNDO_PRINCIPAL = "#1E1E2E"    # Fundo geral escuro
COR_SIDEBAR = "#2B2B40"            # Sidebar mais escura
COR_HEADER = "#3D3D5C"             # Barra de título
COR_CARD = "#363650"               # Cards dos botões
COR_TEXTO = "#FFFFFF"             # Texto claro (branco)
COR_TEXTO_SECUNDARIO = "#FFFFFF"  # Texto secundário
COR_ACCENT = "#6C63FF"            # Cor de destaque (botões)
COR_DANGER = "#E74C3C"            # Vermelho (Truncate)
COR_DANGER_HOVER = "#C0392B"
COR_SUCCESS = "#27AE60"           # Verde (Migrar)
COR_SUCCESS_HOVER = "#219A52"
COR_CONFIG = "#5B86E5"            # Azul (Configurar)
COR_CONFIG_HOVER = "#4A70C4"
COR_LOG_BG = "#1A1A2E"            # Fundo do log
COR_PROGRESS_BG = "#2B2B40"       # Fundo barra de progresso
COR_PROGRESS_FG = "#6C63FF"       # Frente barra de progresso


# =============================================================================
# JANELA DE CONFIGURAÇÕES
# =============================================================================
class JanelaConfiguracoes(ctk.CTkToplevel):
    def __init__(self, parent, *args, **kwargs):
        super().__init__(parent, *args, **kwargs)
        self.parent = parent
        self.title("⚙️ Configurações")
        self.configure(fg_color=COR_FUNDO_PRINCIPAL)

        # Centralizar a janela
        largura_janela = 480
        altura_janela = 580
        largura_tela = self.winfo_screenwidth()
        altura_tela = self.winfo_screenheight()
        pos_x = int((largura_tela / 2) - (largura_janela / 2))
        pos_y = int((altura_tela / 2) - (altura_janela / 2))
        self.geometry(f"{largura_janela}x{altura_janela}+{pos_x}+{pos_y}")
        self.resizable(False, False)

        # Modal
        self.grab_set()

        # Frame com scroll
        self.frame_config = ctk.CTkScrollableFrame(
            self, fg_color=COR_SIDEBAR, corner_radius=12,
            scrollbar_button_color=COR_ACCENT
        )
        self.frame_config.pack(fill="both", expand=True, padx=15, pady=15)

        # ── Seção Firebird ──────────────────────────────────────
        self._criar_titulo_secao("🔥 Conexão Firebird (Origem)")

        self.frame_fb_hp = ctk.CTkFrame(self.frame_config, fg_color="transparent")
        self.frame_fb_hp.pack(fill="x", padx=12, pady=4)

        self.entry_fb_host = self._criar_entry(self.frame_fb_hp, "Host (ex: localhost)", pack_side="left", expand=True)
        self.entry_fb_host.insert(0, os.getenv("FB_HOST", "localhost"))

        self.entry_fb_port = self._criar_entry(self.frame_fb_hp, "Porta", pack_side="right", width=90)
        self.entry_fb_port.insert(0, os.getenv("FB_PORT", "3050"))

        # Caminho do banco + botão Procurar
        self.frame_fb_path = ctk.CTkFrame(self.frame_config, fg_color="transparent")
        self.frame_fb_path.pack(fill="x", padx=12, pady=4)

        self.entry_fb_path = self._criar_entry(self.frame_fb_path, "Caminho do Banco (.fdb / .ibl)", pack_side="left", expand=True)
        if os.getenv("FB_PATH"):
            self.entry_fb_path.insert(0, os.getenv("FB_PATH"))

        self.btn_browse_fb = ctk.CTkButton(
            self.frame_fb_path, text="📁", width=40, height=32,
            fg_color=COR_CARD, hover_color=COR_ACCENT,
            command=self.procurar_banco
        )
        self.btn_browse_fb.pack(side="right", padx=(5, 0))

        self.entry_fb_user = self._criar_entry(self.frame_config, "Usuário (ex: SYSDBA)")
        self.entry_fb_user.insert(0, os.getenv("FB_USER", "SYSDBA"))

        self.entry_fb_pass = self._criar_entry(self.frame_config, "Senha", show="*")
        self.entry_fb_pass.insert(0, os.getenv("FB_PASS", "masterkey"))

        # ── Seção MySQL ─────────────────────────────────────────
        self._criar_titulo_secao("🐬 Conexão MySQL (Destino)")

        self.entry_my_host = self._criar_entry(self.frame_config, "Host (ex: localhost)")
        self.entry_my_host.insert(0, os.getenv("MYSQL_HOST", "localhost"))

        self.entry_my_db = self._criar_entry(self.frame_config, "Nome do Banco de Dados")
        if os.getenv("MYSQL_DB"):
            self.entry_my_db.insert(0, os.getenv("MYSQL_DB"))

        self.entry_my_user = self._criar_entry(self.frame_config, "Usuário (ex: root)")
        self.entry_my_user.insert(0, os.getenv("MYSQL_USER", "root"))

        self.entry_my_pass = self._criar_entry(self.frame_config, "Senha", show="*")
        if os.getenv("MYSQL_PASS"):
            self.entry_my_pass.insert(0, os.getenv("MYSQL_PASS"))

        # ── Botão Testar ────────────────────────────────────────
        self.btn_test_conn = ctk.CTkButton(
            self.frame_config, text="🔌  Testar Conexões", height=42,
            font=("", 14, "bold"),
            fg_color=COR_CONFIG, hover_color=COR_CONFIG_HOVER,
            command=self.teste_conexoes
        )
        self.btn_test_conn.pack(fill="x", padx=12, pady=(25, 10))

    # ── Helpers ──────────────────────────────────────────────────
    def _criar_titulo_secao(self, texto):
        lbl = ctk.CTkLabel(
            self.frame_config, text=texto,
            font=("", 15, "bold"), text_color=COR_ACCENT, anchor="w"
        )
        lbl.pack(fill="x", padx=12, pady=(18, 6))

    def _criar_entry(self, parent, placeholder, pack_side=None, expand=False, width=None, show=None):
        kwargs = {"placeholder_text": placeholder, "height": 32, "fg_color": COR_FUNDO_PRINCIPAL,
                  "border_color": COR_CARD, "text_color": COR_TEXTO, "corner_radius": 8}
        if width:
            kwargs["width"] = width
        if show:
            kwargs["show"] = show

        entry = ctk.CTkEntry(parent, **kwargs)
        if pack_side:
            entry.pack(side=pack_side, fill="x" if expand else None, expand=expand, padx=(0, 5) if pack_side == "left" else 0)
        else:
            entry.pack(fill="x", padx=12, pady=4)
        return entry

    def procurar_banco(self):
        caminho_arquivo = ctk.filedialog.askopenfilename(
            title="Selecione o banco de dados Firebird",
            filetypes=[("Firebird Database", "*.fdb *.ibl"), ("Todos os arquivos", "*.*")]
        )
        if caminho_arquivo:
            self.entry_fb_path.delete(0, "end")
            self.entry_fb_path.insert(0, caminho_arquivo)
            env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
            if not os.path.exists(env_path):
                open(env_path, 'a').close()
            set_key(env_path, "FB_PATH", caminho_arquivo)

    def teste_conexoes(self):
        self.parent.log_message("\n> [Iniciando] Testando Conexões...")

        path_fb = self.entry_fb_path.get()
        user_fb = self.entry_fb_user.get()
        pass_fb = self.entry_fb_pass.get()
        host_fb = self.entry_fb_host.get()
        port_fb = self.entry_fb_port.get()

        if not path_fb:
            self.parent.log_message("⚠️ Caminho do Firebird está vazio.")
            return

        port_fb_int = int(port_fb) if port_fb.isdigit() else 3050

        fb = ConexaoFirebird(path_fb, user_fb, pass_fb, host_fb, port_fb_int)
        sucesso_fb, msg_fb = fb.conectar()

        if sucesso_fb:
            self.parent.log_message(f"✅ {msg_fb}")
            fb.desconectar()
        else:
            self.parent.log_message(f"❌ {msg_fb}")
            return

        host_my = self.entry_my_host.get()
        user_my = self.entry_my_user.get()
        pass_my = self.entry_my_pass.get()
        db_my = self.entry_my_db.get()

        if not db_my:
            self.parent.log_message("⚠️ Nome do banco de dados MySQL está vazio.")
            return

        my = ConexaoMySQL(host_my, user_my, pass_my, db_my)
        sucesso_my, msg_my = my.conectar()

        if sucesso_my:
            self.parent.log_message(f"✅ {msg_my}")
            my.desconectar()
        else:
            self.parent.log_message(f"❌ {msg_my}")
            return

        # Salvar no .env
        env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
        if not os.path.exists(env_path):
            open(env_path, 'a').close()

        set_key(env_path, "FB_HOST", host_fb)
        set_key(env_path, "FB_PORT", port_fb)
        set_key(env_path, "FB_PATH", path_fb)
        set_key(env_path, "FB_USER", user_fb)
        set_key(env_path, "FB_PASS", pass_fb)

        set_key(env_path, "MYSQL_HOST", host_my)
        if db_my: set_key(env_path, "MYSQL_DB", db_my)
        set_key(env_path, "MYSQL_USER", user_my)
        set_key(env_path, "MYSQL_PASS", pass_my)

        self.parent.log_message("🎉 Conexões testadas com sucesso! Configurações salvas.")
        # Habilita os botões de migração
        self.parent.conexoes_ok = True
        self.parent.atualizar_estado_botoes()
        # Carrega as empresas do Firebird no seletor
        self.parent.carregar_empresas_firebird(
            path_fb, user_fb, pass_fb, host_fb, port_fb_int
        )
        self.destroy()


# =============================================================================
# APLICAÇÃO PRINCIPAL
# =============================================================================
class AppMigrador(ctk.CTk):
    def __init__(self):
        super().__init__()

        self.title("Migrador Futura")
        self.configure(fg_color=COR_FUNDO_PRINCIPAL)

        # ── Centralizar janela ───────────────────────────────────
        largura_janela = 1050
        altura_janela = 700
        largura_tela = self.winfo_screenwidth()
        altura_tela = self.winfo_screenheight()
        pos_x = int((largura_tela / 2) - (largura_janela / 2))
        pos_y = int((altura_tela / 2) - (altura_janela / 2))
        self.geometry(f"{largura_janela}x{altura_janela}+{pos_x}+{pos_y}")

        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("blue")

        # Estado interno
        self.conexoes_ok = False
        self.id_loja = None  # Em memória, nunca salvo no .env
        self._log_file = None  # Handle do arquivo de log ativo

        # Spinner / animação de loading
        self._spinner_ativo = False
        self._spinner_frames = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
        self._spinner_idx = 0
        self._spinner_after_id = None

        # ── Layout principal: 3 linhas (header, corpo, footer) ──
        self.grid_rowconfigure(0, weight=0)  # header
        self.grid_rowconfigure(1, weight=1)  # corpo
        self.grid_rowconfigure(2, weight=0)  # footer
        self.grid_columnconfigure(0, weight=0)  # sidebar
        self.grid_columnconfigure(1, weight=1)  # log

        # =============================================================
        # HEADER
        # =============================================================
        self.frame_header = ctk.CTkFrame(self, fg_color=COR_HEADER, corner_radius=0, height=50)
        self.frame_header.grid(row=0, column=0, columnspan=2, sticky="ew")
        self.frame_header.grid_propagate(False)

        self.lbl_titulo = ctk.CTkLabel(
            self.frame_header, text="⚡  Migrador Futura  ⚡",
            font=("", 22, "bold"), text_color="#FFFFFF"
        )
        self.lbl_titulo.pack(expand=True)

        # =============================================================
        # SIDEBAR (coluna esquerda)
        # =============================================================
        self.frame_sidebar = ctk.CTkFrame(self, fg_color=COR_SIDEBAR, corner_radius=0, width=230)
        self.frame_sidebar.grid(row=1, column=0, sticky="nsew")
        self.frame_sidebar.grid_propagate(False)

        # ── ID Loja ────────────────────────────────────────────
        self.frame_idloja = ctk.CTkFrame(self.frame_sidebar, fg_color=COR_CARD, corner_radius=10)
        self.frame_idloja.pack(fill="x", padx=12, pady=(15, 4))

        lbl_idloja_icon = ctk.CTkLabel(
            self.frame_idloja, text="🏪  ID Loja",
            font=("", 13, "bold"), text_color=COR_TEXTO, anchor="w"
        )
        lbl_idloja_icon.pack(fill="x", padx=12, pady=(8, 2))

        self.entry_idloja = ctk.CTkEntry(
            self.frame_idloja, placeholder_text="Nº da Loja",
            height=32, fg_color=COR_FUNDO_PRINCIPAL,
            border_color=COR_ACCENT, text_color=COR_TEXTO,
            justify="center", font=("", 14, "bold"), corner_radius=8
        )
        self.entry_idloja.pack(fill="x", padx=12, pady=(2, 10))

        # ── Seletor de Empresa (populado após testar conexão) ──────────
        self.frame_idempresa = ctk.CTkFrame(self.frame_sidebar, fg_color=COR_CARD, corner_radius=10)
        self.frame_idempresa.pack(fill="x", padx=12, pady=(0, 8))

        lbl_empresa = ctk.CTkLabel(
            self.frame_idempresa, text="🏢  Empresa (Firebird)",
            font=("", 13, "bold"), text_color=COR_TEXTO, anchor="w"
        )
        lbl_empresa.pack(fill="x", padx=12, pady=(8, 2))

        # Dicionário: label-2-linhas → C.ID | label-2-linhas → label-1-linha
        self._empresas_map: dict[str, int] = {}
        self._empresas_short: dict[str, str] = {}
        self._empresa_selecionada_id: int | None = None

        self.opt_empresa = ctk.CTkOptionMenu(
            self.frame_idempresa,
            values=["(Configure os bancos primeiro)"],
            height=52,
            fg_color=COR_FUNDO_PRINCIPAL,
            button_color=COR_ACCENT,
            button_hover_color=COR_CONFIG,
            text_color=COR_TEXTO,
            font=("", 11),
            corner_radius=8,
            state="disabled",
            command=self._ao_selecionar_empresa,
        )
        self.opt_empresa.pack(fill="x", padx=12, pady=(2, 10))

        # ── Botão Configurar ──────────────────────────────────────
        self.btn_config = ctk.CTkButton(
            self.frame_sidebar, text="⚙️  Configurar Bancos",
            font=("", 13, "bold"), height=40,
            fg_color=COR_CONFIG, hover_color=COR_CONFIG_HOVER,
            text_color="#FFFFFF", corner_radius=10,
            command=self.abrir_configuracoes
        )
        self.btn_config.pack(fill="x", padx=12, pady=(8, 5))

        # ── Separador ────────────────────────────────────────────
        sep1 = ctk.CTkFrame(self.frame_sidebar, fg_color=COR_CARD, height=2)
        sep1.pack(fill="x", padx=20, pady=10)

        # ── Botão Migrar Cliente ─────────────────────────────────
        self.frame_mig_cliente = self._criar_botao_migracao(
            emoji="👥", titulo="Migrar Cliente",
            cor_btn=COR_SUCCESS, cor_hover=COR_SUCCESS_HOVER,
            comando=self.iniciar_migracao_clientes
        )

        # ── Separador ────────────────────────────────────────────
        sep2 = ctk.CTkFrame(self.frame_sidebar, fg_color=COR_CARD, height=2)
        sep2.pack(fill="x", padx=20, pady=10)

        # ── Botão Migrar Venda ───────────────────────────────────
        self.frame_mig_venda = self._criar_botao_migracao(
            emoji="🛒", titulo="Migrar Venda",
            cor_btn=COR_SUCCESS, cor_hover=COR_SUCCESS_HOVER,
            comando=self.iniciar_migracao_vendas
        )

        # ── Separador ────────────────────────────────────────────
        sep3 = ctk.CTkFrame(self.frame_sidebar, fg_color=COR_CARD, height=2)
        sep3.pack(fill="x", padx=20, pady=10)

        # ── Botão Truncate ───────────────────────────────────────
        self.btn_truncate = ctk.CTkButton(
            self.frame_sidebar, text="❌  Truncate",
            font=("", 13, "bold"), height=40,
            fg_color=COR_DANGER, hover_color=COR_DANGER_HOVER,
            text_color="#FFFFFF", text_color_disabled="#AAAAAA",
            corner_radius=10,
            command=self.executar_truncate
        )
        self.btn_truncate.pack(fill="x", padx=12, pady=(5, 10))

        # ── Espaçador inferior (empurra crédito para baixo) ──────
        spacer = ctk.CTkFrame(self.frame_sidebar, fg_color="transparent")
        spacer.pack(fill="both", expand=True)

        lbl_versao = ctk.CTkLabel(
            self.frame_sidebar, text="v1.0 • Python + CustomTkinter",
            font=("", 10), text_color=COR_TEXTO_SECUNDARIO
        )
        lbl_versao.pack(pady=(0, 10))

        # =============================================================
        # PAINEL DIREITO (Log)
        # =============================================================
        self.frame_log = ctk.CTkFrame(self, fg_color=COR_FUNDO_PRINCIPAL, corner_radius=0)
        self.frame_log.grid(row=1, column=1, sticky="nsew", padx=(0, 0), pady=(0, 0))

        self.lbl_log_title = ctk.CTkLabel(
            self.frame_log, text="📋  Log da Migração",
            font=("", 17, "bold"), text_color=COR_TEXTO, anchor="w"
        )
        self.lbl_log_title.pack(fill="x", padx=15, pady=(12, 8))

        self.txt_log = ctk.CTkTextbox(
            self.frame_log, font=("Consolas", 12),
            fg_color=COR_LOG_BG, text_color="#C8C8DC",
            corner_radius=10, border_width=1, border_color=COR_CARD,
            wrap="word"
        )
        self.txt_log.pack(fill="both", expand=True, padx=15, pady=(0, 10))

        # =============================================================
        # FOOTER (Barra de Progresso)
        # =============================================================
        self.frame_footer = ctk.CTkFrame(self, fg_color=COR_HEADER, corner_radius=0, height=35)
        self.frame_footer.grid(row=2, column=0, columnspan=2, sticky="ew")
        self.frame_footer.grid_propagate(False)

        self.lbl_progress_pct = ctk.CTkLabel(
            self.frame_footer, text="0%",
            font=("", 12, "bold"), text_color=COR_TEXTO, width=50
        )
        self.lbl_progress_pct.pack(side="left", padx=(15, 5))

        self.progress_bar = ctk.CTkProgressBar(
            self.frame_footer, height=14,
            fg_color=COR_PROGRESS_BG, progress_color=COR_PROGRESS_FG,
            corner_radius=7
        )
        self.progress_bar.pack(side="left", fill="x", expand=True, padx=(0, 10), pady=8)
        self.progress_bar.set(0)

        self.lbl_progress_info = ctk.CTkLabel(
            self.frame_footer, text="%",
            font=("", 12, "bold"), text_color=COR_TEXTO, width=30
        )
        self.lbl_progress_info.pack(side="right", padx=(0, 15))

        # ── Log inicial ─────────────────────────────────────────
        self.log_message("Sistema iniciado.")
        self.log_message("Clique em '⚙️ Configurar Bancos' para começar.\n")

        # Estado inicial dos botões
        self.atualizar_estado_botoes()

    # =================================================================
    # MÉTODOS AUXILIARES
    # =================================================================

    def _criar_botao_migracao(self, emoji, titulo, cor_btn, cor_hover, comando):
        """Cria um card com botão de migração + checkbox de truncate."""
        frame = ctk.CTkFrame(self.frame_sidebar, fg_color=COR_CARD, corner_radius=10)
        frame.pack(fill="x", padx=12, pady=4)

        btn = ctk.CTkButton(
            frame, text=f"{emoji}  {titulo}",
            font=("", 13, "bold"), height=38,
            fg_color=cor_btn, hover_color=cor_hover,
            text_color="#FFFFFF", text_color_disabled="#AAAAAA",
            corner_radius=8, command=comando
        )
        btn.pack(fill="x", padx=8, pady=(8, 4))

        # Checkbox truncate dentro do card
        chk_frame = ctk.CTkFrame(frame, fg_color="transparent")
        chk_frame.pack(fill="x", padx=8, pady=(0, 8))

        chk_var = ctk.BooleanVar(value=False)
        chk = ctk.CTkCheckBox(
            chk_frame, text="Truncar antes",
            variable=chk_var, font=("", 11),
            text_color="#FFFFFF",
            checkbox_width=18, checkbox_height=18,
            corner_radius=4
        )
        chk.pack(anchor="w")

        # Guardar referências
        frame.btn = btn
        frame.chk_var = chk_var
        return frame

    def atualizar_estado_botoes(self):
        """Habilita/desabilita botões de migração conforme o estado."""
        estado = "normal" if self.conexoes_ok else "disabled"
        self.frame_mig_cliente.btn.configure(state=estado)
        self.frame_mig_venda.btn.configure(state=estado)
        self.btn_truncate.configure(state=estado)

    def _ao_selecionar_empresa(self, label_2linhas: str):
        """Callback do OptionMenu: troca exibição para 1 linha e guarda o C.ID."""
        self._empresa_selecionada_id = self._empresas_map.get(label_2linhas)
        short = self._empresas_short.get(label_2linhas, label_2linhas)
        # Atualiza o texto exibido sem alterar os values internos
        self.opt_empresa.set(short)
        self.opt_empresa.configure(height=32)

    def carregar_empresas_firebird(self, path_fb, user_fb, pass_fb, host_fb, port_fb):
        """Consulta o Firebird para listar empresas e popula o OptionMenu."""
        import threading

        def _carregar():
            try:
                fb = ConexaoFirebird(path_fb, user_fb, pass_fb, host_fb, port_fb)
                sucesso, msg = fb.conectar()
                if not sucesso:
                    self.log_message(f"⚠️ Não foi possível carregar empresas: {msg}")
                    return

                query = """
                    SELECT
                        C.ID,
                        C.CNPJ_CPF,
                        C.RAZAO_SOCIAL,
                        COALESCE(P.QUANTIDADE_VENDA, 0) AS QUANTIDADE_VENDA
                    FROM CADASTRO C
                    LEFT JOIN (
                        SELECT FK_EMPRESA, COUNT(ID) AS QUANTIDADE_VENDA
                        FROM PEDIDO
                        WHERE (FK_TIPO_PEDIDO = '1' OR FK_TIPO_PEDIDO = '5')
                        GROUP BY FK_EMPRESA
                    ) P ON P.FK_EMPRESA = C.ID
                    WHERE C.CHK_EMPRESA = 'S'
                """
                cur = fb.conn.cursor()
                cur.execute(query)
                rows = cur.fetchall()
                cur.close()
                fb.desconectar()

                self._empresas_map.clear()
                self._empresas_short.clear()
                self._empresa_selecionada_id = None
                opcoes = []
                for row in rows:
                    cid, cnpj, razao, qtd = row
                    cnpj_fmt = (cnpj or "").strip() or "(sem CNPJ/CPF)"
                    razao_fmt = (razao or "").strip()[:26]
                    label_2 = f"ID: {cid}  |  {razao_fmt}\n{cnpj_fmt}  |  {qtd} vendas"
                    label_1 = f"ID: {cid}  |  {razao_fmt}"
                    self._empresas_map[label_2] = int(cid)
                    self._empresas_short[label_2] = label_1
                    opcoes.append(label_2)

                if not opcoes:
                    opcoes = ["(Nenhuma empresa encontrada)"]

                # Atualizar na thread principal
                def _atualizar_ui():
                    self.opt_empresa.configure(values=opcoes, state="normal", height=52)
                    # Pré-seleciona a primeira opção
                    primeiro_label_2 = opcoes[0]
                    primeiro_label_1 = self._empresas_short.get(primeiro_label_2, primeiro_label_2)
                    self._empresa_selecionada_id = self._empresas_map.get(primeiro_label_2)
                    self.opt_empresa.set(primeiro_label_1)
                    self.opt_empresa.configure(height=32)
                    self.log_message(f"🏢 {len(self._empresas_map)} empresa(s) carregada(s) no seletor.")

                self.after(0, _atualizar_ui)

            except Exception as e:
                self.log_message(f"⚠️ Erro ao carregar empresas: {e}")

        threading.Thread(target=_carregar, daemon=True).start()

    def validar_id_loja(self):
        """Valida se o ID Loja foi informado. Retorna o valor ou None."""
        valor = self.entry_idloja.get().strip()
        if not valor:
            messagebox.showwarning(
                "ID Loja Obrigatório",
                "Por favor, informe o número da Loja (ID Loja) antes de continuar.\n\n"
                "Este campo é obrigatório para a migração.",
                parent=self
            )
            self.entry_idloja.focus_set()
            return None

        if not valor.isdigit():
            messagebox.showerror(
                "ID Loja Inválido",
                "O ID Loja deve ser um número inteiro.",
                parent=self
            )
            self.entry_idloja.focus_set()
            return None

        self.id_loja = int(valor)
        return self.id_loja

    def log_message(self, mensagem):
        """Exibe mensagem na UI e grava no arquivo de log ativo (se houver)."""
        self.txt_log.insert("end", mensagem + "\n")
        self.txt_log.see("end")
        if self._log_file:
            try:
                ts = datetime.datetime.now().strftime("%H:%M:%S")
                self._log_file.write(f"[{ts}] {mensagem}\n")
                self._log_file.flush()
            except Exception:
                pass

    def _abrir_log_arquivo(self, tipo: str) -> str:
        """Cria pasta logs/ e abre um novo arquivo de log para a migração."""
        base_dir = os.path.dirname(os.path.abspath(__file__))
        logs_dir = os.path.join(base_dir, "logs")
        os.makedirs(logs_dir, exist_ok=True)
        caminho = os.path.join(logs_dir, f"{tipo}.log")
        self._log_file = open(caminho, "w", encoding="utf-8")
        self._log_file.write(f"=== Log de Migração [{tipo.upper()}] - {datetime.datetime.now()} ===\n\n")
        return caminho

    def _fechar_log_arquivo(self):
        """Fecha e descarta o handle do arquivo de log atual."""
        if self._log_file:
            try:
                self._log_file.write("\n=== Fim do log ===\n")
                self._log_file.close()
            except Exception:
                pass
            finally:
                self._log_file = None

    def atualizar_progresso(self, valor, texto=None):
        """Atualiza barra de progresso (0.0 a 1.0) e label de percentual."""
        self.progress_bar.configure(mode="determinate")
        self.progress_bar.set(valor)
        pct = int(valor * 100)
        self.lbl_progress_pct.configure(text=f"{pct}%")
        if texto:
            self.lbl_progress_info.configure(text=texto)

    def iniciar_spinner(self, texto="Processando..."):
        """Inicia animação de spinner no footer e barra indeterminada."""
        self._spinner_ativo = True
        self._spinner_texto = texto
        self.progress_bar.configure(mode="indeterminate")
        self.progress_bar.start()
        self._animar_spinner()

    def _animar_spinner(self):
        """Loop interno do spinner."""
        if not self._spinner_ativo:
            return
        frame = self._spinner_frames[self._spinner_idx % len(self._spinner_frames)]
        self.lbl_progress_pct.configure(text=f"{frame} {self._spinner_texto}")
        self.lbl_progress_info.configure(text=frame)
        self._spinner_idx += 1
        self._spinner_after_id = self.after(100, self._animar_spinner)

    def parar_spinner(self):
        """Para a animação do spinner."""
        self._spinner_ativo = False
        if self._spinner_after_id:
            self.after_cancel(self._spinner_after_id)
            self._spinner_after_id = None
        self.progress_bar.stop()
        self.progress_bar.configure(mode="determinate")
        self.progress_bar.set(0)
        self.lbl_progress_pct.configure(text="0%")
        self.lbl_progress_info.configure(text="%")

    # =================================================================
    # AÇÕES
    # =================================================================

    def abrir_configuracoes(self):
        JanelaConfiguracoes(self)

    def iniciar_migracao_clientes(self):
        # Validar ID Loja
        id_loja = self.validar_id_loja()
        if id_loja is None:
            return

        self.log_message("\n🚀 ===============================================")
        self.log_message(f"🚀 [Iniciando] Migração de Clientes (Loja: {id_loja})")
        self.log_message("🚀 ===============================================")
        self.frame_mig_cliente.btn.configure(state="disabled")
        self.iniciar_spinner("Migrando Clientes...")

        # Recuperar conexões do .env
        load_dotenv(override=True)
        path_fb = os.getenv("FB_PATH")
        user_fb = os.getenv("FB_USER")
        pass_fb = os.getenv("FB_PASS")
        host_fb = os.getenv("FB_HOST", "localhost")
        port_fb = int(os.getenv("FB_PORT", 3050))

        host_my = os.getenv("MYSQL_HOST")
        user_my = os.getenv("MYSQL_USER")
        pass_my = os.getenv("MYSQL_PASS")
        db_my = os.getenv("MYSQL_DB")

        fb = ConexaoFirebird(path_fb, user_fb, pass_fb, host_fb, port_fb)
        my = ConexaoMySQL(host_my, user_my, pass_my, db_my)

        migrador = MigradorClientes(fb, my, log_callback=self.log_message)

        truncar_antes = self.frame_mig_cliente.chk_var.get()

        def thread_migracao():
            caminho_log = self._abrir_log_arquivo("clientes")
            self.log_message(f"📄 Log salvo em: {caminho_log}")
            try:
                sucesso = migrador.executar(truncar=truncar_antes)
                if sucesso:
                    self.after(0, lambda: self.parar_spinner())
                    self.after(50, lambda: self.atualizar_progresso(1.0, "✅ Concluído!"))
                    self.log_message("✅ Migração de Clientes concluída.")
                else:
                    self.after(0, lambda: self.parar_spinner())
                    self.after(50, lambda: self.atualizar_progresso(0, "❌ Falhou"))
                    self.log_message("❌ Migração de Clientes falhou.")
            except Exception as e:
                self.log_message(f"❌ Erro inesperado: {e}")
            finally:
                self._fechar_log_arquivo()
                self.after(0, lambda: self.frame_mig_cliente.btn.configure(state="normal"))

        threading.Thread(target=thread_migracao, daemon=True).start()

    def validar_id_empresa(self):
        """Retorna o C.ID da empresa selecionada no OptionMenu, ou None."""
        fk = self._empresa_selecionada_id
        if not fk:
            messagebox.showwarning(
                "Empresa não selecionada",
                "Por favor, configure os bancos e selecione uma Empresa antes de migrar vendas.\n\n"
                "Clique em '⚙️ Configurar Bancos' e teste as conexões para carregar a lista.",
                parent=self
            )
            return None
        return fk

    def iniciar_migracao_vendas(self):
        # Validar ID Loja
        id_loja = self.validar_id_loja()
        if id_loja is None:
            return

        # Validar ID Empresa
        fk_empresa = self.validar_id_empresa()
        if fk_empresa is None:
            return

        self.log_message("\n🛒 ===============================================")
        self.log_message(f"🛒 [Iniciando] Migração de Vendas (Loja: {id_loja} | Empresa FB: {fk_empresa})")
        self.log_message("🛒 ===============================================")
        self.frame_mig_venda.btn.configure(state="disabled")
        self.iniciar_spinner("Extraindo dados...")

        load_dotenv(override=True)
        path_fb = os.getenv("FB_PATH")
        user_fb = os.getenv("FB_USER")
        pass_fb = os.getenv("FB_PASS")
        host_fb = os.getenv("FB_HOST", "localhost")
        port_fb = int(os.getenv("FB_PORT", 3050))

        host_my = os.getenv("MYSQL_HOST")
        user_my = os.getenv("MYSQL_USER")
        pass_my = os.getenv("MYSQL_PASS")
        db_my   = os.getenv("MYSQL_DB")

        fb = ConexaoFirebird(path_fb, user_fb, pass_fb, host_fb, port_fb)
        my = ConexaoMySQL(host_my, user_my, pass_my, db_my)

        # ── Callback de progresso ─────────────────────────────────────
        # progress(0, total) → para spinner, inicia modo determinado
        # progress(idx, total) → atualiza barra + label "X / total"
        def _progresso(atual: int, total: int):
            if total == 0:
                return
            pct = atual / total

            def _atualizar():
                if atual == 0:
                    # Total conhecido: parar spinner e iniciar modo determinado
                    self.parar_spinner()
                    self.progress_bar.configure(mode="determinate")
                    self.progress_bar.set(0)
                    self.lbl_progress_pct.configure(text="0%")
                    self.lbl_progress_info.configure(text=f"0 / {total:,}")
                else:
                    self.progress_bar.set(pct)
                    self.lbl_progress_pct.configure(text=f"{int(pct * 100)}%")
                    self.lbl_progress_info.configure(text=f"{atual:,} / {total:,}")

            self.after(0, _atualizar)

        migrador = MigradorVendas(
            fb_conn=fb,
            my_conn=my,
            id_loja=id_loja,
            fk_empresa=fk_empresa,
            log_callback=self.log_message,
            progress_callback=_progresso,
        )

        truncar_antes = self.frame_mig_venda.chk_var.get()

        def thread_migracao_venda():
            caminho_log = self._abrir_log_arquivo("vendas")
            self.log_message(f"📄 Log salvo em: {caminho_log}")
            try:
                sucesso = migrador.executar(truncar=truncar_antes)
                if sucesso:
                    self.after(0, lambda: self.atualizar_progresso(1.0, "✅ Concluído!"))
                    self.log_message(
                        f"✅ Migração de Vendas+Itens concluída. "
                        f"{len(migrador.mapa_idvenda):,} vendas migradas."
                    )
                else:
                    self.after(0, lambda: self.parar_spinner())
                    self.after(50, lambda: self.atualizar_progresso(0, "❌ Falhou"))
                    self.log_message("❌ Migração de Vendas falhou.")
            except Exception as e:
                self.log_message(f"❌ Erro inesperado: {e}")
            finally:
                self._fechar_log_arquivo()
                self.after(0, lambda: self.frame_mig_venda.btn.configure(state="normal"))

        threading.Thread(target=thread_migracao_venda, daemon=True).start()


    def executar_truncate(self):
        """Trunca as tabelas relacionadas no MySQL destino."""
        id_loja = self.validar_id_loja()
        if id_loja is None:
            return

        confirmar = messagebox.askyesno(
            "⚠️ Confirmar Truncate",
            "ATENÇÃO: Esta ação irá APAGAR todos os dados das tabelas de migração no MySQL destino.\n\n"
            "Deseja continuar?",
            parent=self
        )
        if not confirmar:
            return

        self.log_message("\n❌ ===============================================")
        self.log_message("❌ [TRUNCATE] Limpando tabelas no MySQL destino...")
        self.log_message("❌ ===============================================")

        load_dotenv(override=True)
        host_my = os.getenv("MYSQL_HOST")
        user_my = os.getenv("MYSQL_USER")
        pass_my = os.getenv("MYSQL_PASS")
        db_my = os.getenv("MYSQL_DB")

        my = ConexaoMySQL(host_my, user_my, pass_my, db_my)
        sucesso, msg = my.conectar()

        if not sucesso:
            self.log_message(f"❌ Erro ao conectar no MySQL: {msg}")
            return

        tabelas = ["item", "venda", "cliente"]  # item antes de venda por FK

        try:
            cursor = my.conn.cursor()
            for tabela in tabelas:
                try:
                    cursor.execute(f"TRUNCATE TABLE {tabela}")
                    self.log_message(f"  ✅ Tabela '{tabela}' truncada com sucesso.")
                except Exception as e:
                    self.log_message(f"  ❌ Erro ao truncar '{tabela}': {e}")
            my.conn.commit()
            cursor.close()
        finally:
            my.desconectar()

        self.log_message("✅ Truncate finalizado.\n")


# =============================================================================
# MAIN
# =============================================================================
if __name__ == "__main__":
    app = AppMigrador()
    app.mainloop()
