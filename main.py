import os
import datetime
import customtkinter as ctk
from dotenv import load_dotenv, set_key
from tkinter import messagebox
import threading
import json

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
        self.title("⚙️ Hub de Configurações")
        self.configure(fg_color=COR_FUNDO_PRINCIPAL)

        # Aumentar para caber o menu lateral
        largura_janela = 800
        altura_janela = 600
        largura_tela = self.winfo_screenwidth()
        altura_tela = self.winfo_screenheight()
        pos_x = int((largura_tela / 2) - (largura_janela / 2))
        pos_y = int((altura_tela / 2) - (altura_janela / 2))
        self.geometry(f"{largura_janela}x{altura_janela}+{pos_x}+{pos_y}")
        self.resizable(False, False)
        self.grab_set()

        # Layout Principal: Esquerda (Sidebar) + Direita (Conteúdo)
        self.grid_columnconfigure(0, weight=0) # Sidebar
        self.grid_columnconfigure(1, weight=1) # Conteúdo
        self.grid_rowconfigure(0, weight=1)

        # --- SIDEBAR ---
        self.sidebar = ctk.CTkFrame(self, width=200, corner_radius=0, fg_color=COR_SIDEBAR)
        self.sidebar.grid(row=0, column=0, sticky="nsew")
        self.sidebar.grid_propagate(False)

        self.lbl_menu = ctk.CTkLabel(self.sidebar, text="CONFIGURAÇÕES", font=("", 14, "bold"), text_color=COR_ACCENT)
        self.lbl_menu.pack(pady=(20, 20))

        self.btn_aba_bancos = self._criar_btn_menu("🔌  Bancos", lambda: self.mostrar_aba("bancos"))
        self.btn_aba_params = self._criar_btn_menu("⚙️  Parâmetros", lambda: self.mostrar_aba("params"))
        self.btn_aba_pagam  = self._criar_btn_menu("💳  Pagamentos", lambda: self.mostrar_aba("pagam"))

        # Espaçador para jogar o botão de testar pro final
        ctk.CTkFrame(self.sidebar, fg_color="transparent").pack(fill="both", expand=True)

        self.btn_test = ctk.CTkButton(
            self.sidebar, text="✅  Salvar e Testar", height=48,
            font=("", 14, "bold"), corner_radius=8,
            fg_color=COR_CONFIG, hover_color=COR_CONFIG_HOVER,
            command=self.teste_conexoes
        )
        self.btn_test.pack(fill="x", padx=15, pady=(0, 20))

        # --- CONTAINER DE CONTEÚDO ---
        self.container = ctk.CTkFrame(self, corner_radius=0, fg_color="transparent")
        self.container.grid(row=0, column=1, sticky="nsew", padx=20, pady=20)

        # Abas (Frames)
        self.abas = {}
        self._inicializar_abas()
        
        # Iniciar na primeira aba
        self.mostrar_aba("bancos")

    def _criar_btn_menu(self, texto, comando):
        btn = ctk.CTkButton(
            self.sidebar, text=texto, anchor="w",
            fg_color="transparent", text_color=COR_TEXTO_SECUNDARIO,
            hover_color=COR_CARD, height=45, corner_radius=8,
            font=("", 14, "bold"), command=comando
        )
        btn.pack(fill="x", padx=15, pady=4)
        return btn

    def _inicializar_abas(self):
        # Aba 1: Bancos
        f_bancos = ctk.CTkScrollableFrame(self.container, fg_color="transparent")
        self.abas["bancos"] = f_bancos
        self._montar_aba_bancos(f_bancos)

        # Aba 2: Parâmetros
        f_params = ctk.CTkFrame(self.container, fg_color="transparent")
        self.abas["params"] = f_params
        self._montar_aba_params(f_params)

        # Aba 3: Pagamentos
        f_pagam = ctk.CTkFrame(self.container, fg_color="transparent")
        self.abas["pagam"] = f_pagam
        self._montar_aba_pagam(f_pagam)

    def mostrar_aba(self, nome):
        for n, frame in self.abas.items():
            if n == nome:
                frame.pack(fill="both", expand=True)
            else:
                frame.pack_forget()

        if nome == "pagam":
            if not getattr(self, "pagam_carregado", False):
                self.frame_map.carregar_dados()
                self.pagam_carregado = True
        
        # Highlight no botão
        self.btn_aba_bancos.configure(fg_color=COR_CARD if nome == "bancos" else "transparent", text_color=COR_TEXTO if nome == "bancos" else COR_TEXTO_SECUNDARIO)
        self.btn_aba_params.configure(fg_color=COR_CARD if nome == "params" else "transparent", text_color=COR_TEXTO if nome == "params" else COR_TEXTO_SECUNDARIO)
        self.btn_aba_pagam.configure(fg_color=COR_CARD if nome == "pagam" else "transparent", text_color=COR_TEXTO if nome == "pagam" else COR_TEXTO_SECUNDARIO)

    # --- MONTAGEM DAS ABAS ---

    def _montar_aba_bancos(self, frame):
        # Container com padding extra
        container = ctk.CTkFrame(frame, fg_color="transparent")
        container.pack(fill="both", expand=True, padx=20, pady=10)

        self._criar_titulo_secao(container, "🔥  Conexão Firebird (Origem)")
        
        # Grid para Host e Porta (2 colunas)
        f_fb_h_p = ctk.CTkFrame(container, fg_color="transparent")
        f_fb_h_p.pack(fill="x", pady=(0, 10))
        f_fb_h_p.grid_columnconfigure(0, weight=3)
        f_fb_h_p.grid_columnconfigure(1, weight=1)

        self.entry_fb_host = self._criar_entry(f_fb_h_p, "Host (Ex: localhost)")
        self.entry_fb_host.insert(0, os.getenv("FB_HOST", "localhost"))
        self.entry_fb_host.grid(row=0, column=0, sticky="ew", padx=(0, 10))

        self.entry_fb_port = self._criar_entry(f_fb_h_p, "Porta")
        self.entry_fb_port.insert(0, os.getenv("FB_PORT", "3050"))
        self.entry_fb_port.grid(row=0, column=1, sticky="ew")

        # Layout do Caminho + Botão
        path_f = ctk.CTkFrame(container, fg_color="transparent")
        path_f.pack(fill="x", pady=(0, 10))
        self.entry_fb_path = self._criar_entry(path_f, "Caminho Absoluto do Banco (.fdb / .ibl)", pack_side="left", expand=True)
        if os.getenv("FB_PATH"): self.entry_fb_path.insert(0, os.getenv("FB_PATH"))
        ctk.CTkButton(
            path_f, text="📁", width=45, height=38,
            fg_color=COR_CARD, hover_color=COR_ACCENT, command=self.procurar_banco
        ).pack(side="right", padx=(10, 0))

        # Grid para Usuário e Senha
        f_fb_u_p = ctk.CTkFrame(container, fg_color="transparent")
        f_fb_u_p.pack(fill="x", pady=(0, 20))
        f_fb_u_p.grid_columnconfigure((0, 1), weight=1)

        self.entry_fb_user = self._criar_entry(f_fb_u_p, "Usuário (Ex: SYSDBA)")
        self.entry_fb_user.insert(0, os.getenv("FB_USER", "SYSDBA"))
        self.entry_fb_user.grid(row=0, column=0, sticky="ew", padx=(0, 10))

        self.entry_fb_pass = self._criar_entry(f_fb_u_p, "Senha", show="*")
        self.entry_fb_pass.insert(0, os.getenv("FB_PASS", "masterkey"))
        self.entry_fb_pass.grid(row=0, column=1, sticky="ew")

        # Divisor sutil
        ctk.CTkFrame(container, height=2, fg_color=COR_CARD).pack(fill="x", pady=5)

        self.entry_fb_user = self._criar_entry(frame, "Usuário")
        self.entry_fb_user.insert(0, os.getenv("FB_USER", "SYSDBA"))
        self.entry_fb_pass = self._criar_entry(frame, "Senha", show="*")
        self.entry_fb_pass.insert(0, os.getenv("FB_PASS", "masterkey"))

        self._criar_titulo_secao(container, "🐬  Conexão MySQL (Destino)")
        
        # Grid Host/Banco
        f_my_h_d = ctk.CTkFrame(container, fg_color="transparent")
        f_my_h_d.pack(fill="x", pady=(0, 10))
        f_my_h_d.grid_columnconfigure((0, 1), weight=1)

        self.entry_my_host = self._criar_entry(f_my_h_d, "Host (Ex: localhost)")
        self.entry_my_host.insert(0, os.getenv("MYSQL_HOST", "localhost"))
        self.entry_my_host.grid(row=0, column=0, sticky="ew", padx=(0, 10))

        self.entry_my_db = self._criar_entry(f_my_h_d, "Banco de Dados")
        if os.getenv("MYSQL_DB"): self.entry_my_db.insert(0, os.getenv("MYSQL_DB"))
        self.entry_my_db.grid(row=0, column=1, sticky="ew")

        # Grid User/Pass
        f_my_u_p = ctk.CTkFrame(container, fg_color="transparent")
        f_my_u_p.pack(fill="x", pady=(0, 20))
        f_my_u_p.grid_columnconfigure((0, 1), weight=1)

        self.entry_my_user = self._criar_entry(f_my_u_p, "Usuário (Ex: root)")
        self.entry_my_user.insert(0, os.getenv("MYSQL_USER", "root"))
        self.entry_my_user.grid(row=0, column=0, sticky="ew", padx=(0, 10))

        self.entry_my_pass = self._criar_entry(f_my_u_p, "Senha", show="*")
        if os.getenv("MYSQL_PASS"): self.entry_my_pass.insert(0, os.getenv("MYSQL_PASS"))
        self.entry_my_pass.grid(row=0, column=1, sticky="ew")

    def _montar_aba_params(self, frame):
        container = ctk.CTkFrame(frame, fg_color="transparent")
        container.pack(fill="both", expand=True, padx=30, pady=20)

        self._criar_titulo_secao(container, "⚙️  Parâmetros da Migração")
        
        # ID Loja
        ctk.CTkLabel(container, text="ID da Loja no Destino:", font=("", 13, "bold"), anchor="w").pack(fill="x", pady=(15, 5))
        self.entry_id_loja = self._criar_entry(container, "Digite o Nº da Loja")
        if os.getenv("ID_LOJA"): self.entry_id_loja.insert(0, os.getenv("ID_LOJA"))
        self.entry_id_loja.pack(fill="x")

        # Seletor Empresa
        ctk.CTkLabel(container, text="Empresa Selecionada (Origem Firebird):", font=("", 13, "bold"), anchor="w").pack(fill="x", pady=(25, 5))
        
        self.opt_empresa = ctk.CTkOptionMenu(
            container, values=self.parent.opt_empresa.cget("values"), height=45,
            fg_color=COR_FUNDO_PRINCIPAL, button_color=COR_ACCENT, button_hover_color=COR_CONFIG,
            text_color=COR_TEXTO, corner_radius=8, font=("", 13),
            command=self._ao_selecionar_empresa
        )
        self.opt_empresa.pack(fill="x")
        self.opt_empresa.set(self.parent.opt_empresa.get())
        if self.parent.opt_empresa.cget("state") == "disabled":
            self.opt_empresa.configure(state="disabled")

        ctk.CTkLabel(
            container, text="💡 Dica: As empresas aparecem após Clicar em 'Testar Conexões' na aba Bancos.", 
            font=("", 12, "italic"), text_color=COR_TEXTO_SECUNDARIO
        ).pack(fill="x", pady=(15, 0))

    def _montar_aba_pagam(self, frame):
        container = ctk.CTkFrame(frame, fg_color="transparent")
        container.pack(fill="both", expand=True, padx=10, pady=10)

        self._criar_titulo_secao(container, "💳  Mapeamento de Pagamentos")
        
        self.frame_map = FrameMapeamentoPagamento(container, self, fg_color="transparent")
        self.frame_map.pack(fill="both", expand=True)

    # --- HELPERS ---
    def _criar_titulo_secao(self, parent, texto):
        lbl = ctk.CTkLabel(parent, text=texto, font=("", 18, "bold"), text_color=COR_ACCENT, anchor="w")
        lbl.pack(fill="x", pady=(10, 15))

    def _criar_entry(self, parent, placeholder, pack_side=None, expand=False, width=None, show=None):
        entry = ctk.CTkEntry(
            parent, placeholder_text=placeholder, height=38, 
            fg_color=COR_FUNDO_PRINCIPAL, border_color=COR_SIDEBAR, border_width=2,
            text_color=COR_TEXTO, placeholder_text_color="#6F6F80", corner_radius=8, show=show, font=("", 13)
        )
        if width: entry.configure(width=width)
        
        # O retorno abaixo é útil se O PARENT NÃO TIVER SEU PRÓPRIO GERENCIADOR (pack ou grid explicitamente fora daqui)
        # Como estamos usando GRID fora daqui para alguns, o ideal é não empacotar automaticamente se vamos fazer layout manual.
        # Ajuste: se pack_side não for None e expand não for booleano-falso-com-grid... melhor deixar o caller fazer o grid/pack exceto para casos simples.
        if pack_side: 
            entry.pack(side=pack_side, fill="x" if expand else None, expand=expand, padx=(0, 10) if pack_side == "left" else 0)
        
        return entry

    # --- COMANDOS ---

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

    def _ao_selecionar_empresa(self, label: str):
        """Repassa a seleção para o AppMigrador e atualiza a exibição local."""
        self.parent._ao_selecionar_empresa(label)
        self.opt_empresa.set(label)

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

        # Salvar ID Loja
        id_loja = self.entry_id_loja.get().strip()
        if id_loja:
            set_key(env_path, "ID_LOJA", id_loja)
            self.parent.id_loja = int(id_loja) if id_loja.isdigit() else None
            self.parent.entry_idloja_mem = id_loja # Manter em memória p/ consistência

        self.parent.log_message("🎉 Conexões testadas com sucesso! Configurações salvas.")
        # Habilita os botões de migração
        self.parent.conexoes_ok = True
        self.btn_aba_params.pack(fill="x", padx=15, pady=4)
        self.btn_aba_pagam.pack(fill="x", padx=15, pady=4)
        self.pagam_carregado = False # Força recarregamento caso alterem os bancos
        self.parent.atualizar_estado_botoes()
        # Carrega as empresas do Firebird - agora atualiza tanto o hub quanto a sidebar (se houver)
        self.parent.carregar_empresas_firebird(
            path_fb, user_fb, pass_fb, host_fb, port_fb_int,
            callback_ui=lambda opts, sel: self.opt_empresa.configure(values=opts, state="normal") or self.opt_empresa.set(sel)
        )


# =============================================================================
# FRAME DE MAPEAMENTO DE PAGAMENTO (MASTER-DETAIL AUTO-ADVANCE)
# =============================================================================
class FrameMapeamentoPagamento(ctk.CTkFrame):
    def __init__(self, parent_container, parent_config, *args, **kwargs):
        super().__init__(parent_container, *args, **kwargs)
        self.parent_config = parent_config

        # Layout Base: 2 colunas principais e 1 linha para o footer
        self.grid_rowconfigure(0, weight=1) # Área de trabalho
        self.grid_rowconfigure(1, weight=0) # Footer (Botão Salvar)
        self.grid_columnconfigure(0, weight=1) # Lista Origem
        self.grid_columnconfigure(1, weight=1) # Lista Destino / Busca

        # ─── Frame Esquerdo (Master - Origem FB) ──────────────────────
        self.frame_esq = ctk.CTkFrame(self, fg_color="transparent")
        self.frame_esq.grid(row=0, column=0, sticky="nsew", padx=(20, 10), pady=(20, 10))
        
        ctk.CTkLabel(
            self.frame_esq, text="🔥 Origem (Firebird)", 
            font=("", 16, "bold"), text_color=COR_ACCENT, anchor="w"
        ).pack(fill="x", pady=(0, 10))

        self.scroll_esq = ctk.CTkScrollableFrame(
            self.frame_esq, fg_color=COR_SIDEBAR, corner_radius=12,
            scrollbar_button_color=COR_ACCENT
        )
        self.scroll_esq.pack(fill="both", expand=True)

        # ─── Frame Direito (Detail - Destino MySQL) ───────────────────
        self.frame_dir = ctk.CTkFrame(self, fg_color="transparent")
        self.frame_dir.grid(row=0, column=1, sticky="nsew", padx=(10, 20), pady=(20, 10))

        self.lbl_info_dir = ctk.CTkLabel(
            self.frame_dir, text="🐬 Destino (Selecione uma forma à esquerda)", 
            font=("", 16, "bold"), text_color=COR_ACCENT, anchor="w"
        )
        self.lbl_info_dir.pack(fill="x", pady=(0, 10))

        # Barra de Busca (embutida)
        self.entry_busca = ctk.CTkEntry(
            self.frame_dir, placeholder_text="🔍 Buscar Plano MySQL (Digite para filtrar)...",
            height=40, fg_color=COR_SIDEBAR, border_color=COR_ACCENT,
            border_width=2, text_color=COR_TEXTO, font=("", 14), state="disabled"
        )
        self.entry_busca.pack(fill="x", pady=(0, 15))
        self.entry_busca.bind("<KeyRelease>", self.filtrar_planos)

        self.scroll_dir = ctk.CTkScrollableFrame(
            self.frame_dir, fg_color=COR_SIDEBAR, corner_radius=12,
            scrollbar_button_color=COR_ACCENT
        )
        self.scroll_dir.pack(fill="both", expand=True)

        # Botão Limpar Seleção (Direita)
        self.btn_clear = ctk.CTkButton(
            self.frame_dir, text="🧹 Limpar Seleção Desta Forma", height=38,
            font=("", 13, "bold"), fg_color=COR_DANGER, hover_color=COR_DANGER_HOVER,
            command=self.limpar_selecao_ativa, state="disabled"
        )
        self.btn_clear.pack(fill="x", pady=(15, 0))

        # ─── Footer ───────────────────────────────────────────────────
        f_footer = ctk.CTkFrame(self, fg_color=COR_HEADER, height=60, corner_radius=0)
        f_footer.grid(row=1, column=0, columnspan=2, sticky="ew")
        
        self.btn_save = ctk.CTkButton(
            f_footer, text="💾  Salvar Mapeamento", height=40, width=250,
            font=("", 14, "bold"), fg_color=COR_SUCCESS, hover_color=COR_SUCCESS_HOVER,
            command=self.salvar
        )
        self.btn_save.pack(pady=10)

        # ─── Estado Interno ───────────────────────────────────────────
        self.formas_fb = []
        self.planos_my = [] # [(id, nome), ...]
        self.botoes_planos = [] # Referências aos widgets na direita
        
        self.map_selecionado = {} # { forma_origem: (id_plano, nome_plano) }
        self.map_bts_esq = {}     # { forma_origem: CTkButton } (Botões da esq para atualizar)
        
        self.forma_ativa = None

    def carregar_dados(self):
        """Busca formas no FB e planos no MySQL."""
        path_fb = self.parent_config.entry_fb_path.get()
        user_fb = self.parent_config.entry_fb_user.get()
        pass_fb = self.parent_config.entry_fb_pass.get()
        host_fb = self.parent_config.entry_fb_host.get()
        port_fb = int(self.parent_config.entry_fb_port.get() or 3050)

        host_my = self.parent_config.entry_my_host.get()
        user_my = self.parent_config.entry_my_user.get()
        pass_my = self.parent_config.entry_my_pass.get()
        db_my = self.parent_config.entry_my_db.get()

        try:
            # 1. Planos MySQL
            my = ConexaoMySQL(host_my, user_my, pass_my, db_my)
            if my.conectar()[0]:
                cur = my.conn.cursor()
                cur.execute("SELECT id, nome FROM plano ORDER BY nome")
                self.planos_my = cur.fetchall()
                cur.close()
                my.desconectar()

            # 2. Formas Firebird
            fb = ConexaoFirebird(path_fb, user_fb, pass_fb, host_fb, port_fb)
            if fb.conectar()[0]:
                cur = fb.conn.cursor()
                query = """
                SELECT DISTINCT
                    CASE
                        WHEN CI.FK_CARTAO IS NOT NULL AND CI.FK_CARTAO > 0 THEN C.DESCRICAO
                        ELSE TP.DESCRICAO
                    END AS forma_pagamento
                FROM CAIXA_ITEM CI
                LEFT JOIN CARTAO C ON C.ID = CI.FK_CARTAO
                LEFT JOIN TIPO_PAGAMENTO TP ON TP.ID = CI.FK_TIPO_PAGAMENTO
                """
                cur.execute(query)
                self.formas_fb = [row[0] for row in cur.fetchall() if row[0]]
                cur.close()
                fb.desconectar()

            self.montar_listas()

        except Exception as e:
            messagebox.showerror("Erro", f"Erro ao carregar dados para mapeamento: {e}")

    def montar_listas(self):
        # Limpar listas anteriores
        for widget in self.scroll_esq.winfo_children():
            widget.destroy()
        for widget in self.scroll_dir.winfo_children():
            widget.destroy()
        
        self.map_selecionado.clear()
        self.map_bts_esq.clear()
        self.botoes_planos.clear()
        
        # Mapeamento atual salvo no disco
        current_map = {}
        if os.path.exists("mapping_pagamento.json"):
            with open("mapping_pagamento.json", "r", encoding="utf-8") as f:
                current_map = json.load(f)

        plano_options_map = {p[0]: p[1] for p in self.planos_my}

        # Melhoria de Performance: Criar TODOS os botões da direita uma única vez em memória
        for pid, nome in self.planos_my:
            btn = ctk.CTkButton(
                self.scroll_dir, text=f"{pid} - {nome}",
                fg_color="transparent", text_color=COR_TEXTO, anchor="w",
                hover_color=COR_HEADER, height=35, font=("", 13),
                command=lambda p=(pid, nome): self.selecionar_plano(p)
            )
            self.botoes_planos.append((pid, nome, btn))
            
        self.lbl_mais_opcoes = ctk.CTkLabel(self.scroll_dir, text="... (Digite mais para refinar os resultados)", font=("", 11, "italic"), text_color=COR_TEXTO_SECUNDARIO)

        # Popular Lado Esquerdo (FB)
        for forma in self.formas_fb:
            # Container do item
            item_frame = ctk.CTkFrame(self.scroll_esq, fg_color=COR_CARD, corner_radius=8)
            item_frame.pack(fill="x", pady=4, padx=5)

            # Define estado inicial baseado no JSON
            id_salvo = current_map.get(forma)
            texto_inicial = forma
            cor_texto = COR_TEXTO_SECUNDARIO

            if id_salvo and id_salvo in plano_options_map:
                self.map_selecionado[forma] = (id_salvo, plano_options_map[id_salvo])
                texto_inicial = f"{forma}\n👉 {id_salvo} - {plano_options_map[id_salvo]}"
                cor_texto = COR_SUCCESS # Indica que já está mapeado!

            # Botão que representa a forma inteira
            btn = ctk.CTkButton(
                item_frame, text=texto_inicial, anchor="w",
                font=("", 13, "bold"), height=50,
                fg_color="transparent", text_color=cor_texto, hover_color=COR_HEADER,
                command=lambda f=forma: self.ativar_forma(f)
            )
            btn.pack(fill="both", expand=True, padx=5, pady=5)
            self.map_bts_esq[forma] = btn

        # Inicializa Lado Direito (Vazio até clicar na esquerda)
        self.filtrar_planos() 

        # Auto-selecionar a primeira forma se existir
        if self.formas_fb:
            self.ativar_forma(self.formas_fb[0])

    def ativar_forma(self, forma):
        """Seleciona a forma do Firebird (Esquerda) e prepara a busca (Direita)"""
        self.forma_ativa = forma

        # Destacar o botão à esquerda
        for f, btn in self.map_bts_esq.items():
            if f == forma:
                btn.configure(fg_color=COR_ACCENT, text_color="#FFFFFF")
                # Se desmarcou do COR_SUCCESS acima, restauramos na atualização do texto
            else:
                ja_mapeado = f in self.map_selecionado
                btn.configure(fg_color="transparent", text_color=COR_SUCCESS if ja_mapeado else COR_TEXTO_SECUNDARIO)

        # Atualizar Lado Direito
        self.lbl_info_dir.configure(text=f"📌 Mapeando: {forma}")
        self.entry_busca.configure(state="normal")
        self.btn_clear.configure(state="normal")
        self.entry_busca.delete(0, 'end')
        self.filtrar_planos() # Mostra toda a lista
        self.entry_busca.focus_set()

    def filtrar_planos(self, event=None):
        """Filtra a lista do MySQL com base na busca."""
        if self.forma_ativa is None:
            return

        termo = self.entry_busca.get().lower()

        # Limpar Apenas UI (Forget) - Muito mais rápido do que destruir
        for pid, nome, btn in self.botoes_planos:
            btn.pack_forget()
        if hasattr(self, 'lbl_mais_opcoes'):
            self.lbl_mais_opcoes.pack_forget()

        count = 0
        for pid, nome, btn in self.botoes_planos:
            if termo in str(pid).lower() or termo in nome.lower():
                btn.pack(fill="x", pady=2)
                count += 1
            if count >= 50: # Limite visual de performance
                self.lbl_mais_opcoes.pack(pady=10)
                break

    def selecionar_plano(self, plano_selecionado):
        """Associa o plano MySQL selecionado à forma Firebird ativa."""
        if not self.forma_ativa:
            return
        
        # 1. Salvar Associação
        pid, nome = plano_selecionado
        self.map_selecionado[self.forma_ativa] = (pid, nome)
        
        # 2. Atualizar UI da Esquerda
        btn = self.map_bts_esq[self.forma_ativa]
        btn.configure(text=f"{self.forma_ativa}\n✅ {pid} - {nome}")

        # 3. Avançar para a próxima "não mapaeada" (Auto-Advance UX Magic)
        self.avancar_proxima_nao_mapeada()

    def limpar_selecao_ativa(self):
        """Remove o mapeamento da forma atualmente ativa."""
        if not self.forma_ativa:
            return
        
        self.map_selecionado.pop(self.forma_ativa, None)
        btn = self.map_bts_esq[self.forma_ativa]
        btn.configure(text=self.forma_ativa)
        # Manter foco nela mesma
        self.ativar_forma(self.forma_ativa)

    def avancar_proxima_nao_mapeada(self):
        """Acha a primeira forma sem mapeamento iterando na lista e foca nela."""
        todas_mapeadas = True
        prox = None

        # Primeiro, tentar encontrar a próxima a partir da atual (se não mapeada)
        idx_atual = self.formas_fb.index(self.forma_ativa) if self.forma_ativa in self.formas_fb else -1
        
        for i in range(idx_atual + 1, len(self.formas_fb)):
            if self.formas_fb[i] not in self.map_selecionado:
                prox = self.formas_fb[i]
                break
        
        # Se não achou dali pra frente, procurar desde o início
        if prox is None:
            for f in self.formas_fb:
                if f not in self.map_selecionado:
                    prox = f
                    break
        
        if prox:
            self.ativar_forma(prox)
        else:
            # Todas estão mapeadas! Feedback legal:
            self.forma_ativa = None
            self.lbl_info_dir.configure(text="✅ Todas as formas mapeadas! Pronto para salvar.")
            self.entry_busca.delete(0, 'end')
            self.entry_busca.configure(state="disabled")
            self.btn_clear.configure(state="disabled")
            self.filtrar_planos() # Limpará os botões pois forma_ativa = None
            
            # Tirar highlight esquerdo garantindo cores "success"
            for f, btn in self.map_bts_esq.items():
                btn.configure(fg_color="transparent", text_color=COR_SUCCESS)

    def salvar(self):
        mapping = {}
        for forma, dados in self.map_selecionado.items():
            id_plano, _ = dados
            mapping[forma] = id_plano

        try:
            with open("mapping_pagamento.json", "w", encoding="utf-8") as f:
                json.dump(mapping, f, ensure_ascii=False, indent=2)
            self.parent_config.parent.log_message(f"💾 Mapeamento associado com sucesso: {len(mapping)} formas de pagamento.")
            messagebox.showinfo("Sucesso", "Mapeamento salvo com sucesso!")
        except Exception as e:
            messagebox.showerror("Erro", f"Erro ao salvar mapeamento: {e}")


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
        self.minsize(900, 600)

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
        self.frame_sidebar = ctk.CTkFrame(self, fg_color=COR_SIDEBAR, corner_radius=0, width=250)
        self.frame_sidebar.grid(row=1, column=0, sticky="nsew")
        self.frame_sidebar.grid_propagate(False)

        # ── ID Loja ────────────────────────────────────────────
        # self.frame_idloja = ctk.CTkFrame(self.frame_sidebar, fg_color=COR_CARD, corner_radius=10)
        # Removido da sidebar, mas mantemos o objeto p/ compatibilidade se necessário, ou limpamos
        # self.frame_idloja.pack(fill="x", padx=12, pady=(15, 4))
        
        # ID Loja agora vem do .env ou Config
        self.id_loja = int(os.getenv("ID_LOJA")) if os.getenv("ID_LOJA") and os.getenv("ID_LOJA").isdigit() else None
        self.entry_idloja_mem = os.getenv("ID_LOJA", "") # Cache em memória

        # ── Seletor de Empresa (Interno - não exibido na sidebar mais) ──────────
        self._empresas_map: dict[str, int] = {}
        self._empresas_short: dict[str, str] = {}
        self._empresa_selecionada_id: int | None = None

        self.opt_empresa = ctk.CTkOptionMenu(
            self, # Sem parent visível na sidebar
            values=["(Configure os bancos primeiro)"],
            height=32,
            fg_color=COR_FUNDO_PRINCIPAL,
            button_color=COR_ACCENT,
            button_hover_color=COR_CONFIG,
            text_color=COR_TEXTO,
            font=("", 11),
            corner_radius=8,
            state="disabled",
            command=self._ao_selecionar_empresa,
        )
        # Não damos pack() nele na sidebar principal, ele fica oculto

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
        frame = ctk.CTkFrame(self.frame_sidebar, fg_color=COR_CARD, corner_radius=12)
        frame.pack(fill="x", padx=15, pady=6)

        btn = ctk.CTkButton(
            frame, text=f"{emoji}  {titulo}",
            font=("", 14, "bold"), height=42,
            fg_color=cor_btn, hover_color=cor_hover,
            text_color="#FFFFFF", text_color_disabled="#AAAAAA",
            corner_radius=8, command=comando
        )
        btn.pack(fill="x", padx=10, pady=(10, 5))

        # Checkbox truncate dentro do card
        chk_frame = ctk.CTkFrame(frame, fg_color="transparent")
        chk_frame.pack(fill="x", padx=8, pady=(0, 8))

        chk_var = ctk.BooleanVar(value=False)
        chk = ctk.CTkCheckBox(
            chk_frame, text="Truncar destino antes",
            variable=chk_var, font=("", 12),
            text_color="#E0E0E0", hover_color=COR_HEADER,
            checkbox_width=20, checkbox_height=20,
            corner_radius=6
        )
        chk.pack(anchor="w", pady=(0, 5))

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

    def _ao_selecionar_empresa(self, label: str):
        """Callback do OptionMenu: guarda o C.ID selecionado."""
        self._empresa_selecionada_id = self._empresas_map.get(label)
        self.opt_empresa.set(label)

    def carregar_empresas_firebird(self, path_fb, user_fb, pass_fb, host_fb, port_fb, callback_ui=None):
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
                    cnpj_fmt = (cnpj or "").strip() or "(s/ CNPJ)"
                    razao_fmt = (razao or "").strip()[:26]
                    # Formatado em uma única linha para evitar bug no CTkOptionMenu
                    label = f"ID: {cid} | {razao_fmt} | {cnpj_fmt} | {qtd} vendas"
                    
                    self._empresas_map[label] = int(cid)
                    # Não precisamos mais do dicionário de "short" porque a string se auto resolve sem conflito
                    opcoes.append(label)

                if not opcoes:
                    opcoes = ["(Nenhuma empresa encontrada)"]

                # Atualizar na thread principal
                def _atualizar_ui():
                    self.opt_empresa.configure(values=opcoes, state="normal")
                    primeiro_label = opcoes[0]
                    self._empresa_selecionada_id = self._empresas_map.get(primeiro_label)
                    self.opt_empresa.set(primeiro_label)
                    self.log_message(f"🏢 {len(self._empresas_map)} empresa(s) carregada(s) no seletor.")
                    if callback_ui:
                        callback_ui(opcoes, primeiro_label)

                self.after(0, _atualizar_ui)

            except Exception as e:
                self.log_message(f"⚠️ Erro ao carregar empresas: {e}")

        threading.Thread(target=_carregar, daemon=True).start()

    def validar_id_loja(self):
        """Valida se o ID Loja foi informado. Retorna o valor ou None."""
        if self.id_loja is None:
            messagebox.showwarning(
                "Configuração Incompleta",
                "Por favor, acesse '⚙️ Configurar Bancos' e informe o número da Loja (ID Loja).",
                parent=self
            )
            return None
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
