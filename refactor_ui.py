import os
import re

main_path = "main.py"

with open(main_path, "r", encoding="utf-8") as f:
    content = f.read()

# Remove JanelaConfiguracoes (from "class JanelaConfiguracoes" up to "class FrameMapeamentoPagamento")
pattern_config = re.compile(r"# =+?[\r\n]+# JANELA DE CONFIGURAÇÕES[\r\n]+# =+?[\r\n]+class JanelaConfiguracoes.*?# =+?[\r\n]+# FRAME DE MAPEAMENTO DE PAGAMENTO", re.DOTALL)
content = pattern_config.sub("# =============================================================================\n# FRAME DE MAPEAMENTO DE PAGAMENTO", content)


new_frame_map = """class FrameMapeamentoPagamento(ctk.CTkFrame):
    def __init__(self, parent_container, parent_app, *args, **kwargs):
        super().__init__(parent_container, *args, **kwargs)
        self.parent_app = parent_app

        self.grid_rowconfigure(0, weight=1) # Área de trabalho
        self.grid_rowconfigure(1, weight=0) # Footer
        self.grid_columnconfigure(0, weight=1)
        self.grid_columnconfigure(1, weight=1)

        # ─── Frame Esquerdo (Master - Origem FB)
        self.frame_esq = ctk.CTkFrame(self, fg_color="transparent")
        self.frame_esq.grid(row=0, column=0, sticky="nsew", padx=(20, 10), pady=(20, 10))
        ctk.CTkLabel(self.frame_esq, text="🔥 Formas de Pagamento (Firebird)", font=("", 15, "bold"), text_color=COR_ACCENT, anchor="w").pack(fill="x", pady=(0, 10))
        self.scroll_esq = ctk.CTkScrollableFrame(self.frame_esq, fg_color=COR_SIDEBAR, corner_radius=12, scrollbar_button_color=COR_ACCENT)
        self.scroll_esq.pack(fill="both", expand=True)

        # ─── Frame Direito (Detail - Destino MySQL)
        self.frame_dir = ctk.CTkFrame(self, fg_color="transparent")
        self.frame_dir.grid(row=0, column=1, sticky="nsew", padx=(10, 20), pady=(20, 10))
        self.lbl_info_dir = ctk.CTkLabel(self.frame_dir, text="🐬 Planos no MySQL", font=("", 15, "bold"), text_color=COR_ACCENT, anchor="w")
        self.lbl_info_dir.pack(fill="x", pady=(0, 10))

        self.entry_busca = ctk.CTkEntry(self.frame_dir, placeholder_text="🔍 Filtrar Planos...", height=40, fg_color=COR_SIDEBAR, border_color=COR_ACCENT, border_width=2, text_color=COR_TEXTO, font=("", 14), state="disabled")
        self.entry_busca.pack(fill="x", pady=(0, 15))
        self.entry_busca.bind("<KeyRelease>", self.filtrar_planos)

        self.scroll_dir = ctk.CTkScrollableFrame(self.frame_dir, fg_color=COR_SIDEBAR, corner_radius=12, scrollbar_button_color=COR_ACCENT)
        self.scroll_dir.pack(fill="both", expand=True)

        self.btn_clear = ctk.CTkButton(self.frame_dir, text="🧹 Limpar Seleção", height=38, font=("", 13, "bold"), fg_color=COR_DANGER, hover_color=COR_DANGER_HOVER, command=self.limpar_selecao_ativa, state="disabled")
        self.btn_clear.pack(fill="x", pady=(15, 0))

        # ─── Footer
        f_footer = ctk.CTkFrame(self, fg_color=COR_HEADER, height=60, corner_radius=0)
        f_footer.grid(row=1, column=0, columnspan=2, sticky="ew")
        self.btn_save = ctk.CTkButton(f_footer, text="💾  Salvar Mapeamento e Voltar", height=40, width=280, font=("", 14, "bold"), fg_color=COR_SUCCESS, hover_color=COR_SUCCESS_HOVER, command=self.salvar_e_voltar)
        self.btn_save.pack(pady=10)

        self.formas_fb = []
        self.planos_my = [] 
        self.botoes_planos = [] 
        self.map_selecionado = {} 
        self.map_bts_esq = {}     
        self.forma_ativa = None

    def carregar_dados(self):
        path_fb = self.parent_app.fb_path
        user_fb = os.getenv("FB_USER", "SYSDBA")
        pass_fb = os.getenv("FB_PASS", "masterkey")
        host_fb = os.getenv("FB_HOST", "localhost")
        port_fb = int(os.getenv("FB_PORT", "3050"))

        host_my = os.getenv("MYSQL_HOST", "localhost")
        user_my = os.getenv("MYSQL_USER", "root")
        pass_my = os.getenv("MYSQL_PASS", "")
        db_my = os.getenv("MYSQL_DB", "")

        try:
            from core import ConexaoMySQL, ConexaoFirebird
            my = ConexaoMySQL(host_my, user_my, pass_my, db_my)
            if my.conectar()[0]:
                cur = my.conn.cursor()
                cur.execute("SELECT id, nome FROM plano ORDER BY nome")
                self.planos_my = cur.fetchall()
                cur.close()
                my.desconectar()

            fb = ConexaoFirebird(path_fb, user_fb, pass_fb, host_fb, port_fb)
            if fb.conectar()[0]:
                cur = fb.conn.cursor()
                cur.execute(\"\"\"SELECT DISTINCT CASE WHEN CI.FK_CARTAO IS NOT NULL AND CI.FK_CARTAO > 0 THEN C.DESCRICAO ELSE TP.DESCRICAO END AS forma_pagamento FROM CAIXA_ITEM CI LEFT JOIN CARTAO C ON C.ID = CI.FK_CARTAO LEFT JOIN TIPO_PAGAMENTO TP ON TP.ID = CI.FK_TIPO_PAGAMENTO\"\"\")
                self.formas_fb = [row[0] for row in cur.fetchall() if row[0]]
                cur.close()
                fb.desconectar()
            self.montar_listas()
        except Exception as e:
            messagebox.showerror("Erro", f"Erro ao carregar dados: {e}")

    def montar_listas(self):
        for widget in self.scroll_esq.winfo_children(): widget.destroy()
        for widget in self.scroll_dir.winfo_children(): widget.destroy()
        self.map_selecionado.clear()
        self.map_bts_esq.clear()
        self.botoes_planos.clear()
        current_map = {}
        if os.path.exists("mapping_pagamento.json"):
            import json
            with open("mapping_pagamento.json", "r", encoding="utf-8") as f:
                current_map = json.load(f)
        plano_options_map = {p[0]: p[1] for p in self.planos_my}

        for pid, nome in self.planos_my:
            btn = ctk.CTkButton(self.scroll_dir, text=f"{pid} - {nome}", fg_color="transparent", text_color=COR_TEXTO, anchor="w", hover_color=COR_HEADER, height=35, font=("", 13), command=lambda p=(pid, nome): self.selecionar_plano(p))
            self.botoes_planos.append((pid, nome, btn))
        self.lbl_mais_opcoes = ctk.CTkLabel(self.scroll_dir, text="... (refine na busca)", font=("", 11, "italic"), text_color=COR_TEXTO_SECUNDARIO)

        for forma in self.formas_fb:
            item_frame = ctk.CTkFrame(self.scroll_esq, fg_color=COR_CARD, corner_radius=8)
            item_frame.pack(fill="x", pady=4, padx=5)
            id_salvo = current_map.get(forma)
            texto_inicial = forma
            cor_texto = COR_TEXTO_SECUNDARIO
            if id_salvo and id_salvo in plano_options_map:
                self.map_selecionado[forma] = (id_salvo, plano_options_map[id_salvo])
                texto_inicial = f"{forma}\\\\n👉 {id_salvo} - {plano_options_map[id_salvo][:15]}..."
                cor_texto = COR_SUCCESS
            btn = ctk.CTkButton(item_frame, text=texto_inicial, anchor="w", font=("", 13, "bold"), height=50, fg_color="transparent", text_color=cor_texto, hover_color=COR_HEADER, command=lambda f=forma: self.ativar_forma(f))
            btn.pack(fill="both", expand=True, padx=5, pady=5)
            self.map_bts_esq[forma] = btn
        self.filtrar_planos()
        if self.formas_fb: self.ativar_forma(self.formas_fb[0])

    def ativar_forma(self, forma):
        self.forma_ativa = forma
        for f, btn in self.map_bts_esq.items():
            if f == forma:
                btn.configure(fg_color=COR_ACCENT, text_color="#FFFFFF")
            else:
                ja_mapeado = f in self.map_selecionado
                btn.configure(fg_color="transparent", text_color=COR_SUCCESS if ja_mapeado else COR_TEXTO_SECUNDARIO)
        self.lbl_info_dir.configure(text=f"📌 Mapeando: {forma[:20]}..")
        self.entry_busca.configure(state="normal")
        self.btn_clear.configure(state="normal")
        self.entry_busca.delete(0, 'end')
        self.filtrar_planos()
        self.entry_busca.focus_set()

    def filtrar_planos(self, event=None):
        if self.forma_ativa is None: return
        termo = self.entry_busca.get().lower()
        for pid, nome, btn in self.botoes_planos: btn.pack_forget()
        if hasattr(self, 'lbl_mais_opcoes'): self.lbl_mais_opcoes.pack_forget()
        count = 0
        for pid, nome, btn in self.botoes_planos:
            if termo in str(pid).lower() or termo in nome.lower():
                btn.pack(fill="x", pady=2)
                count += 1
            if count >= 50:
                self.lbl_mais_opcoes.pack(pady=10)
                break

    def selecionar_plano(self, plano_selecionado):
        if not self.forma_ativa: return
        pid, nome = plano_selecionado
        self.map_selecionado[self.forma_ativa] = (pid, nome)
        self.map_bts_esq[self.forma_ativa].configure(text=f"{self.forma_ativa[:15]}..\\\\n✅ {pid} - {nome[:15]}..")
        self.avancar_proxima_nao_mapeada()

    def limpar_selecao_ativa(self):
        if not self.forma_ativa: return
        self.map_selecionado.pop(self.forma_ativa, None)
        self.map_bts_esq[self.forma_ativa].configure(text=self.forma_ativa)
        self.ativar_forma(self.forma_ativa)

    def avancar_proxima_nao_mapeada(self):
        idx_atual = self.formas_fb.index(self.forma_ativa) if self.forma_ativa in self.formas_fb else -1
        prox = next((f for f in self.formas_fb[idx_atual + 1:] if f not in self.map_selecionado), None)
        if prox is None: prox = next((f for f in self.formas_fb if f not in self.map_selecionado), None)
        if prox:
            self.ativar_forma(prox)
        else:
            self.forma_ativa = None
            self.lbl_info_dir.configure(text="✅ Tudo mapeado!")
            self.entry_busca.delete(0, 'end')
            self.entry_busca.configure(state="disabled")
            self.btn_clear.configure(state="disabled")
            self.filtrar_planos()
            for f, btn in self.map_bts_esq.items(): btn.configure(fg_color="transparent", text_color=COR_SUCCESS)

    def salvar_e_voltar(self):
        import json
        mapping = {f: d[0] for f, d in self.map_selecionado.items()}
        try:
            with open("mapping_pagamento.json", "w", encoding="utf-8") as f:
                json.dump(mapping, f, ensure_ascii=False, indent=2)
            self.parent_app.log_message(f"💾 Mapeamento salvo: {len(mapping)} formas.")
            self.parent_app.mostrar_log()
        except Exception as e:
            messagebox.showerror("Erro", f"Erro ao salvar: {e}")
"""

# Replace the old FrameMapeamentoPagamento
pattern_frame = re.compile(r"class FrameMapeamentoPagamento.*?# =============================================================================[\r\n]+# APLICAÇÃO PRINCIPAL", re.DOTALL)
content = pattern_frame.sub(new_frame_map + "\n\n# =============================================================================\n# APLICAÇÃO PRINCIPAL", content)

new_app_migrador = """class AppMigrador(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("Migrador Futura - Assistente Inteligente")
        self.configure(fg_color=COR_FUNDO_PRINCIPAL)
        
        largura_janela, altura_janela = 1100, 700
        pos_x = int((self.winfo_screenwidth() / 2) - (largura_janela / 2))
        pos_y = int((self.winfo_screenheight() / 2) - (altura_janela / 2))
        self.geometry(f"{largura_janela}x{altura_janela}+{pos_x}+{pos_y}")
        self.minsize(900, 600)
        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("blue")

        self.conexoes_ok = False
        self.fb_path = os.getenv("FB_PATH", "")
        self.id_loja = int(os.getenv("ID_LOJA")) if os.getenv("ID_LOJA", "").isdigit() else None
        self._log_file = None
        self._spinner_ativo = False
        self._spinner_idx = 0
        self._spinner_frames = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
        self._spinner_after_id = None
        self._empresas_map = {}
        self._empresa_selecionada_id = None

        self.grid_rowconfigure(0, weight=1)
        self.grid_rowconfigure(1, weight=0)
        self.grid_columnconfigure(0, weight=4) # Left side (Assistente)
        self.grid_columnconfigure(1, weight=5) # Right side (Log/Mapeamento)

        # ── ESQUERDA (Assistente) ──
        self.frame_left = ctk.CTkFrame(self, fg_color=COR_SIDEBAR, corner_radius=0)
        self.frame_left.grid(row=0, column=0, sticky="nsew")
        
        # Header Esquerdo
        f_head = ctk.CTkFrame(self.frame_left, fg_color=COR_HEADER, corner_radius=0, height=60)
        f_head.pack(fill="x")
        f_head.pack_propagate(False)
        ctk.CTkLabel(f_head, text="⚡ Assistente de Migração", font=("", 20, "bold"), text_color="#FFFFFF").pack(expand=True)

        self.scroll_left = ctk.CTkScrollableFrame(self.frame_left, fg_color="transparent")
        self.scroll_left.pack(fill="both", expand=True, padx=20, pady=10)

        # Passo 1: Arquivo
        self._criar_titulo(self.scroll_left, "1. Banco Antigo (Firebird)")
        f_arquivo = ctk.CTkFrame(self.scroll_left, fg_color=COR_CARD, corner_radius=10)
        f_arquivo.pack(fill="x", pady=(0, 20))
        self.btn_procurar = ctk.CTkButton(f_arquivo, text="📂 Escolher Arquivo do Sistema Antigo (.fdb)", font=("", 14, "bold"), height=50, fg_color=COR_ACCENT, hover_color=COR_CONFIG_HOVER, command=self.procurar_fdb)
        self.btn_procurar.pack(fill="x", padx=15, pady=(15, 5))
        self.lbl_arquivo = ctk.CTkLabel(f_arquivo, text=self.fb_path or "Nenhum arquivo selecionado.", font=("", 11), text_color=COR_TEXTO_SECUNDARIO, wraplength=350)
        self.lbl_arquivo.pack(pady=(0, 15), padx=15)

        # Passo 2: Negócio (Oculto até selecionar FDB)
        self.f_passo2 = ctk.CTkFrame(self.scroll_left, fg_color="transparent")
        self._criar_titulo(self.f_passo2, "2. Configuração de Loja")
        f_negocio = ctk.CTkFrame(self.f_passo2, fg_color=COR_CARD, corner_radius=10)
        f_negocio.pack(fill="x", pady=(0, 20))
        
        ctk.CTkLabel(f_negocio, text="Selecione a Empresa:", font=("", 12, "bold"), anchor="w").pack(fill="x", padx=15, pady=(15, 0))
        self.opt_empresa = ctk.CTkOptionMenu(f_negocio, values=[""], height=40, font=("", 13), fg_color=COR_FUNDO_PRINCIPAL, button_color=COR_ACCENT, button_hover_color=COR_CONFIG, command=self._ao_selecionar_empresa)
        self.opt_empresa.pack(fill="x", padx=15, pady=(5, 10))

        ctk.CTkLabel(f_negocio, text="ID da Nova Loja (Futura):", font=("", 12, "bold"), anchor="w").pack(fill="x", padx=15, pady=(5, 0))
        self.entry_id_loja = ctk.CTkEntry(f_negocio, placeholder_text="Ex: 1", height=40, font=("", 13), fg_color=COR_FUNDO_PRINCIPAL, border_width=1)
        self.entry_id_loja.pack(fill="x", padx=15, pady=(5, 15))
        if self.id_loja: self.entry_id_loja.insert(0, str(self.id_loja))

        self.btn_map_pagam = ctk.CTkButton(f_negocio, text="🔗  Revisar Mapas de Pagamento", font=("", 13, "bold"), height=40, fg_color="transparent", border_width=1, border_color=COR_ACCENT, text_color=COR_TEXTO, hover_color=COR_FUNDO_PRINCIPAL, command=self.mostrar_mapeamento)
        self.btn_map_pagam.pack(fill="x", padx=15, pady=(0, 15))

        # Passo 3: Migrar
        self.f_passo3 = ctk.CTkFrame(self.scroll_left, fg_color="transparent")
        self._criar_titulo(self.f_passo3, "3. Ações de Migração")
        
        self.frame_mig_cliente = self._criar_botao_migracao(self.f_passo3, "👥", "Migrar Clientes", self.iniciar_migracao_clientes)
        self.frame_mig_venda = self._criar_botao_migracao(self.f_passo3, "🛒", "Migrar Vendas", self.iniciar_migracao_vendas)
        
        self.btn_truncate = ctk.CTkButton(self.f_passo3, text="❌ Limpar Destino (Truncate)", font=("", 13, "bold"), height=45, fg_color=COR_DANGER, hover_color=COR_DANGER_HOVER, corner_radius=10, command=self.executar_truncate)
        self.btn_truncate.pack(fill="x", pady=10)

        # ── DIREITA (Área Dinâmica: Log / Mapas) ──
        self.frame_right = ctk.CTkFrame(self, fg_color=COR_FUNDO_PRINCIPAL, corner_radius=0)
        self.frame_right.grid(row=0, column=1, sticky="nsew")
        self.frame_right.grid_rowconfigure(1, weight=1)
        self.frame_right.grid_columnconfigure(0, weight=1)

        f_head_rt = ctk.CTkFrame(self.frame_right, fg_color=COR_HEADER, corner_radius=0, height=60)
        f_head_rt.grid(row=0, column=0, sticky="ew")
        f_head_rt.pack_propagate(False)
        self.lbl_right_title = ctk.CTkLabel(f_head_rt, text="📋 Logs e Andamento", font=("", 18, "bold"), text_color=COR_TEXTO)
        self.lbl_right_title.pack(expand=True)

        self.container_dinamico = ctk.CTkFrame(self.frame_right, fg_color="transparent")
        self.container_dinamico.grid(row=1, column=0, sticky="nsew", padx=15, pady=15)

        self.txt_log = ctk.CTkTextbox(self.container_dinamico, font=("Consolas", 12), fg_color=COR_LOG_BG, text_color="#C8C8DC", corner_radius=10, border_width=1, border_color=COR_CARD, wrap="word")
        self.txt_log.pack(fill="both", expand=True)

        self.frame_map = FrameMapeamentoPagamento(self.container_dinamico, self, fg_color="transparent")

        # ── FOOTER (Progresso) ──
        self.frame_footer = ctk.CTkFrame(self, fg_color=COR_HEADER, corner_radius=0, height=40)
        self.frame_footer.grid(row=1, column=0, columnspan=2, sticky="ew")
        self.lbl_progress_pct = ctk.CTkLabel(self.frame_footer, text="0%", font=("", 13, "bold"), text_color=COR_TEXTO, width=50)
        self.lbl_progress_pct.pack(side="left", padx=(15, 5))
        self.progress_bar = ctk.CTkProgressBar(self.frame_footer, height=14, fg_color=COR_PROGRESS_BG, progress_color=COR_PROGRESS_FG, corner_radius=7)
        self.progress_bar.pack(side="left", fill="x", expand=True, padx=(0, 10), pady=13)
        self.progress_bar.set(0)
        self.lbl_progress_info = ctk.CTkLabel(self.frame_footer, text="", font=("", 13, "bold"), text_color=COR_TEXTO, width=30)
        self.lbl_progress_info.pack(side="right", padx=(0, 15))

        self.log_message("Sistema de Migração Ultra Fluido iniciado.")
        if self.fb_path:
            self.testar_conexoes_silencioso(self.fb_path)

    def _criar_titulo(self, parent, texto):
        ctk.CTkLabel(parent, text=texto, font=("", 15, "bold"), text_color=COR_ACCENT, anchor="w").pack(fill="x", pady=(0, 5))

    def _criar_botao_migracao(self, parent, emoji, titulo, comando):
        frame = ctk.CTkFrame(parent, fg_color=COR_CARD, corner_radius=12)
        frame.pack(fill="x", pady=6)
        btn = ctk.CTkButton(frame, text=f"{emoji}  {titulo}", font=("", 15, "bold"), height=45, fg_color=COR_SUCCESS, hover_color=COR_SUCCESS_HOVER, corner_radius=8, command=comando)
        btn.pack(fill="x", padx=10, pady=(10, 5))
        chk_var = ctk.BooleanVar(value=False)
        chk = ctk.CTkCheckBox(frame, text="Truncar tabelas antes", variable=chk_var, font=("", 12), text_color="#E0E0E0", hover_color=COR_HEADER, checkbox_width=20, checkbox_height=20)
        chk.pack(anchor="w", padx=10, pady=(0, 10))
        frame.btn = btn
        frame.chk_var = chk_var
        return frame

    def procurar_fdb(self):
        caminho = ctk.filedialog.askopenfilename(title="Selecione o banco FDB da Origem", filetypes=[("Firebird DB", "*.fdb *.ibl"), ("Todos", "*.*")])
        if caminho:
            self.fb_path = caminho
            self.lbl_arquivo.configure(text=caminho)
            from dotenv import set_key
            env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
            set_key(env_path, "FB_PATH", caminho)
            self.testar_conexoes_silencioso(caminho)

    def testar_conexoes_silencioso(self, fb_path):
        self.log_message(f"\\\\nTestando conexão com arquivo selecionado...")
        user_fb = os.getenv("FB_USER", "SYSDBA")
        pass_fb = os.getenv("FB_PASS", "masterkey")
        host_fb = os.getenv("FB_HOST", "localhost")
        port_fb = int(os.getenv("FB_PORT", "3050"))
        
        host_my = os.getenv("MYSQL_HOST", "localhost")
        user_my = os.getenv("MYSQL_USER", "root")
        pass_my = os.getenv("MYSQL_PASS", "")
        db_my = os.getenv("MYSQL_DB", "")

        from core import ConexaoFirebird, ConexaoMySQL
        fb = ConexaoFirebird(fb_path, user_fb, pass_fb, host_fb, port_fb)
        sucesso_fb, msg_fb = fb.conectar()
        if sucesso_fb:
            self.log_message(f"✅ Firebird: OK!")
            fb.desconectar()
        else:
            self.log_message(f"❌ Firebird Erro: {msg_fb}")
            return
            
        my = ConexaoMySQL(host_my, user_my, pass_my, db_my)
        sucesso_my, msg_my = my.conectar()
        if sucesso_my:
            self.log_message(f"✅ MySQL: OK! Ambiente pronto.")
            my.desconectar()
        else:
            self.log_message(f"❌ MySQL Erro: Verifique o .env! O banco de destino falhou. {msg_my}")
            return

        self.conexoes_ok = True
        self.btn_procurar.configure(fg_color=COR_SUCCESS, text="✅ Arquivo Selecionado Configurado")
        self.f_passo2.pack(fill="x", pady=(0, 20))
        self.f_passo3.pack(fill="x", pady=(0, 20))
        
        import threading
        # Carregar empresas automaticamente
        def load_emp():
            try:
                fb = ConexaoFirebird(fb_path, user_fb, pass_fb, host_fb, port_fb)
                fb.conectar()
                cur = fb.conn.cursor()
                cur.execute("SELECT C.ID, C.CNPJ_CPF, C.RAZAO_SOCIAL, COALESCE(P.QUANTIDADE_VENDA, 0) FROM CADASTRO C LEFT JOIN (SELECT FK_EMPRESA, COUNT(ID) AS QUANTIDADE_VENDA FROM PEDIDO WHERE (FK_TIPO_PEDIDO = '1' OR FK_TIPO_PEDIDO = '5') GROUP BY FK_EMPRESA) P ON P.FK_EMPRESA = C.ID WHERE C.CHK_EMPRESA = 'S'")
                rows = cur.fetchall()
                cur.close()
                fb.desconectar()
                opcoes = []
                self._empresas_map.clear()
                for r in rows:
                    lbl = f"ID:{r[0]} | {str(r[2])[:20]} | Vendas: {r[3]}"
                    self._empresas_map[lbl] = int(r[0])
                    opcoes.append(lbl)
                if not opcoes: opcoes = ["(Nenhuma)"]
                self.after(0, lambda: self.opt_empresa.configure(values=opcoes) or self.opt_empresa.set(opcoes[0]) or self._ao_selecionar_empresa(opcoes[0]))
            except Exception as e:
                self.log_message(f"Aviso: Erro lendo empresas: {e}")
        threading.Thread(target=load_emp, daemon=True).start()

    def mostrar_mapeamento(self):
        self.txt_log.pack_forget()
        self.lbl_right_title.configure(text="💳 Configuração de Pagamentos")
        self.frame_map.pack(fill="both", expand=True)
        if not getattr(self, "pagam_carregado", False):
            self.frame_map.carregar_dados()
            self.pagam_carregado = True

    def mostrar_log(self):
        self.frame_map.pack_forget()
        self.lbl_right_title.configure(text="📋 Logs e Andamento")
        self.txt_log.pack(fill="both", expand=True)

    def _ao_selecionar_empresa(self, label: str):
        self._empresa_selecionada_id = self._empresas_map.get(label)
        self.opt_empresa.set(label)

    def salvar_id_loja(self):
        val = self.entry_id_loja.get().strip()
        if val.isdigit():
            self.id_loja = int(val)
            from dotenv import set_key
            env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
            set_key(env_path, "ID_LOJA", val)
            return True
        else:
            messagebox.showwarning("Aviso", "O ID da Loja deve ser numérico.")
            return False

    def log_message(self, mensagem):
        self.txt_log.insert("end", mensagem + "\\\\n")
        self.txt_log.see("end")
        if self._log_file:
            try:
                import datetime
                self._log_file.write(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {mensagem}\\\\n")
                self._log_file.flush()
            except Exception: pass

    def _abrir_log_arquivo(self, tipo: str) -> str:
        import datetime
        logs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
        os.makedirs(logs_dir, exist_ok=True)
        caminho = os.path.join(logs_dir, f"{tipo}.log")
        self._log_file = open(caminho, "w", encoding="utf-8")
        self._log_file.write(f"=== Log [{tipo.upper()}] - {datetime.datetime.now()} ===\\\\n\\\\n")
        return caminho

    def _fechar_log_arquivo(self):
        if self._log_file:
            try:
                self._log_file.close()
            except Exception: pass
            finally: self._log_file = None

    def atualizar_progresso(self, valor, texto=None):
        self.progress_bar.configure(mode="determinate")
        self.progress_bar.set(valor)
        self.lbl_progress_pct.configure(text=f"{int(valor * 100)}%")
        if texto: self.lbl_progress_info.configure(text=texto)

    def iniciar_spinner(self, texto="Processando..."):
        self._spinner_ativo = True
        self._spinner_texto = texto
        self.progress_bar.configure(mode="indeterminate")
        self.progress_bar.start()
        self._animar_spinner()

    def _animar_spinner(self):
        if not self._spinner_ativo: return
        frame = self._spinner_frames[self._spinner_idx % len(self._spinner_frames)]
        self.lbl_progress_pct.configure(text=f"{frame} {self._spinner_texto}")
        self._spinner_idx += 1
        self._spinner_after_id = self.after(100, self._animar_spinner)

    def parar_spinner(self):
        self._spinner_ativo = False
        if self._spinner_after_id:
            self.after_cancel(self._spinner_after_id)
            self._spinner_after_id = None
        self.progress_bar.stop()
        self.progress_bar.configure(mode="determinate")
        self.progress_bar.set(0)
        self.lbl_progress_pct.configure(text="0%")
        self.lbl_progress_info.configure(text="")

    def iniciar_migracao_clientes(self):
        if not self.salvar_id_loja(): return
        self.mostrar_log()
        self.log_message("\\\\n🚀 [Iniciando] Migração de Clientes (Loja: {})".format(self.id_loja))
        self.frame_mig_cliente.btn.configure(state="disabled")
        self.iniciar_spinner("Migrando Clientes...")

        from core import ConexaoFirebird, ConexaoMySQL
        from migrador_clientes import MigradorClientes
        fb = ConexaoFirebird(self.fb_path, os.getenv("FB_USER", "SYSDBA"), os.getenv("FB_PASS", "masterkey"), os.getenv("FB_HOST", "localhost"), int(os.getenv("FB_PORT", "3050")))
        my = ConexaoMySQL(os.getenv("MYSQL_HOST"), os.getenv("MYSQL_USER"), os.getenv("MYSQL_PASS"), os.getenv("MYSQL_DB"))
        migrador = MigradorClientes(fb, my, log_callback=self.log_message)
        truncar_antes = self.frame_mig_cliente.chk_var.get()

        def run():
            self._abrir_log_arquivo("clientes")
            try:
                sucesso = migrador.executar(truncar=truncar_antes)
                self.after(0, lambda: self.parar_spinner())
                self.after(50, lambda: self.atualizar_progresso(1.0, "✅ Concluído!" if sucesso else "❌ Falhou"))
            except Exception as e:
                self.log_message(f"❌ Erro: {e}")
            finally:
                self._fechar_log_arquivo()
                self.after(0, lambda: self.frame_mig_cliente.btn.configure(state="normal"))
        import threading
        threading.Thread(target=run, daemon=True).start()

    def iniciar_migracao_vendas(self):
        if not self.salvar_id_loja(): return
        if not self._empresa_selecionada_id:
            messagebox.showwarning("Aviso", "Selecione a empresa de origem.")
            return
        self.mostrar_log()
        self.log_message("\\\\n🛒 [Iniciando] Migração de Vendas (Loja: {})".format(self.id_loja))
        self.frame_mig_venda.btn.configure(state="disabled")
        self.iniciar_spinner("Extraindo...")

        from core import ConexaoFirebird, ConexaoMySQL
        from migrador_vendas import MigradorVendas
        fb = ConexaoFirebird(self.fb_path, os.getenv("FB_USER", "SYSDBA"), os.getenv("FB_PASS", "masterkey"), os.getenv("FB_HOST", "localhost"), int(os.getenv("FB_PORT", "3050")))
        my = ConexaoMySQL(os.getenv("MYSQL_HOST"), os.getenv("MYSQL_USER"), os.getenv("MYSQL_PASS"), os.getenv("MYSQL_DB"))

        def _progresso(atual, total):
            if total == 0: return
            pct = atual / total
            if atual == 0:
                self.after(0, lambda: self.parar_spinner() or self.progress_bar.configure(mode="determinate") or self.progress_bar.set(0) or self.lbl_progress_info.configure(text=f"0 / {total:,}"))
            else:
                self.after(0, lambda: self.progress_bar.set(pct) or self.lbl_progress_pct.configure(text=f"{int(pct * 100)}%") or self.lbl_progress_info.configure(text=f"{atual:,} / {total:,}"))

        migrador = MigradorVendas(fb, my, self.id_loja, self._empresa_selecionada_id, self.log_message, _progresso)
        truncar_antes = self.frame_mig_venda.chk_var.get()

        def run():
            self._abrir_log_arquivo("vendas")
            try:
                sucesso = migrador.executar(truncar=truncar_antes)
                if sucesso:
                    self.after(0, lambda: self.atualizar_progresso(1.0, "✅ Concluído!"))
                else:
                    self.after(0, lambda: self.parar_spinner() or self.atualizar_progresso(0, "❌ Falhou"))
            except Exception as e:
                self.log_message(f"❌ Erro: {e}")
            finally:
                self._fechar_log_arquivo()
                self.after(0, lambda: self.frame_mig_venda.btn.configure(state="normal"))
        import threading
        threading.Thread(target=run, daemon=True).start()

    def executar_truncate(self):
        if not messagebox.askyesno("⚠️ Truncate", "APAGARÁ todos os clientes e vendas no destino. Continuar?"): return
        self.mostrar_log()
        self.log_message("\\\\n❌ Limpando tabelas...")
        host_my = os.getenv("MYSQL_HOST")
        user_my = os.getenv("MYSQL_USER")
        pass_my = os.getenv("MYSQL_PASS")
        db_my = os.getenv("MYSQL_DB")
        from core import ConexaoMySQL
        my = ConexaoMySQL(host_my, user_my, pass_my, db_my)
        sucesso, msg = my.conectar()
        if not sucesso:
            self.log_message(f"❌ Erro: {msg}")
            return
        tabelas = ["item", "venda", "cliente"]
        try:
            cursor = my.conn.cursor()
            for tabela in tabelas:
                try:
                    cursor.execute(f"TRUNCATE TABLE {tabela}")
                    self.log_message(f"  ✅ Tabela '{tabela}' limpa.")
                except Exception as e:
                    self.log_message(f"  ❌ Erro ao limpar '{tabela}': {e}")
            my.conn.commit()
            cursor.close()
        finally:
            my.desconectar()
        self.log_message("✅ Finalizado.\\\\n")

# =============================================================================
# MAIN
# =============================================================================
if __name__ == "__main__":
    app = AppMigrador()
    app.mainloop()
"""

pattern_main = re.compile(r"class AppMigrador\(ctk\.CTk\):.*", re.DOTALL)
content = pattern_main.sub(new_app_migrador, content)

with open(main_path, "w", encoding="utf-8") as f:
    f.write(content)
print("MAIN.PY WRITTEN SUCCESSFULLY")
