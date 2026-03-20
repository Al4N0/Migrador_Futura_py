import customtkinter as ctk

# Paleta de Cores (Estilo Web ERP / Tailwind Dark Mode)
BG_COLOR = "#0B0F19"         # Fundo escuro profundo
SIDEBAR_COLOR = "#111827"    # Fundo da barra lateral
CARD_COLOR = "#1F2937"       # Cards de conteúdo
PRIMARY = "#4F46E5"          # Roxo/Azul vibrante (Ação principal)
PRIMARY_HOVER = "#4338CA"
SUCCESS = "#10B981"          # Verde suave (Status OK)
TEXT_WHITE = "#F9FAFB"       # Texto claro principal
TEXT_MUTED = "#9CA3AF"       # Texto secundário (cinza)
BORDER_COLOR = "#374151"     # Limites e bordas finas

class WebStyleMockup(ctk.CTk):
    def __init__(self):
        super().__init__()
        
        # Configuração da Janela
        self.title("Playground - Nova Interface Visual (Sem Scroll)")
        self.geometry("1100x760")
        self.configure(fg_color=BG_COLOR)
        
        # O CustomTkinter permite definir o tema base
        ctk.set_appearance_mode("dark")

        # ─── Grid Principal (2 Colunas: Sidebar e Conteúdo)
        self.grid_rowconfigure(0, weight=1)
        self.grid_columnconfigure(1, weight=1) # A coluna 1 expande (Conteúdo principal)

        self._montar_sidebar()
        self._montar_area_principal()

    def _montar_sidebar(self):
        # Frame da Barra Lateral
        self.sidebar = ctk.CTkFrame(self, width=260, corner_radius=0, fg_color=SIDEBAR_COLOR)
        self.sidebar.grid(row=0, column=0, sticky="nsew")
        self.sidebar.grid_propagate(False) # Mantém a largura fixa de 260px
        
        # Título / Logo
        lbl_logo = ctk.CTkLabel(
            self.sidebar, text="⚡ Migrador Pro", 
            font=("Segoe UI", 24, "bold"), text_color=TEXT_WHITE
        )
        lbl_logo.pack(pady=(30, 40))

        # Menu de Navegação
        self._criar_nav_btn(self.sidebar, "🏠  Visão Geral", ativo=True)
        self._criar_nav_btn(self.sidebar, "⚙️  Conexões BD", ativo=False)
        self._criar_nav_btn(self.sidebar, "💳  Mapeamentos", ativo=False)
        self._criar_nav_btn(self.sidebar, "📋  Logs de Sistema", ativo=False)

        # Empurrar o restate para baixo e colocar uma versão
        ctk.CTkFrame(self.sidebar, fg_color="transparent").pack(expand=True, fill="both")
        
        lbl_footer = ctk.CTkLabel(
            self.sidebar, text="Versão 2.5 - Design System", 
            font=("Segoe UI", 11), text_color=TEXT_MUTED
        )
        lbl_footer.pack(side="bottom", pady=20)

    def _criar_nav_btn(self, parent, texto, ativo=False):
        cor_fundo = CARD_COLOR if ativo else "transparent"
        cor_texto = TEXT_WHITE if ativo else TEXT_MUTED
        
        btn = ctk.CTkButton(
            parent, text=texto, height=45, corner_radius=8, anchor="w",
            font=("Segoe UI", 15, "bold" if ativo else "normal"),
            fg_color=cor_fundo, hover_color=CARD_COLOR, text_color=cor_texto
        )
        btn.pack(fill="x", padx=15, pady=4)

    def _montar_area_principal(self):
        # Container Direito Principal
        self.main_area = ctk.CTkFrame(self, fg_color="transparent")
        self.main_area.grid(row=0, column=1, sticky="nsew")
        self.main_area.grid_columnconfigure(0, weight=1)
        self.main_area.grid_rowconfigure(1, weight=1) # Faz o conteúdo expandir

        # ─── HEADER (Topo Superior)
        self.header = ctk.CTkFrame(self.main_area, height=70, corner_radius=0, fg_color=BG_COLOR)
        self.header.grid(row=0, column=0, sticky="ew")
        
        lbl_breadcrumb = ctk.CTkLabel(
            self.header, text="Dashboard  >  Configuração de Migração", 
            font=("Segoe UI", 13), text_color=TEXT_MUTED
        )
        lbl_breadcrumb.pack(side="left", padx=30, pady=25)

        lbl_status = ctk.CTkLabel(
            self.header, text="🟢 Bases Conectadas", 
            font=("Segoe UI", 12, "bold"), text_color=SUCCESS,
            fg_color="#064E3B", corner_radius=5, padx=10, pady=5
        )
        lbl_status.pack(side="right", padx=30, pady=20)

        # ─── CORPO (Cards e Formulários)
        self.main_body = ctk.CTkFrame(
            self.main_area, fg_color="transparent"
        )
        self.main_body.grid(row=1, column=0, sticky="nsew", padx=20, pady=10)

        # Título da Página
        ctk.CTkLabel(
            self.main_body, text="Parâmetros da Migração", 
            font=("Segoe UI", 28, "bold"), text_color=TEXT_WHITE, anchor="w"
        ).pack(fill="x", padx=10, pady=(0, 25))

        # --- CARDS DE MÉTRICAS (Lado a lado usando um grid interno)
        f_metricas = ctk.CTkFrame(self.main_body, fg_color="transparent")
        f_metricas.pack(fill="x", padx=10, pady=(0, 20))
        f_metricas.grid_columnconfigure((0, 1), weight=1)

        self._criar_metrica_card(f_metricas, 0, "Registros Locais (FDB)", "92.650", "Vendas lidas na origem")
        self._criar_metrica_card(f_metricas, 1, "Plataforma Alvo (MySQL)", "Online", "Latência: 12ms")

        # --- CARD DE FORMULÁRIO GIGANTE
        self.card_forms = ctk.CTkFrame(
            self.main_body, fg_color=CARD_COLOR, corner_radius=12,
            border_width=1, border_color=BORDER_COLOR
        )
        self.card_forms.pack(fill="both", expand=True, padx=10, pady=10)

        ctk.CTkLabel(
            self.card_forms, text="Configuração Inicial", 
            font=("Segoe UI", 18, "bold"), text_color=TEXT_WHITE, anchor="w"
        ).pack(fill="x", padx=25, pady=(25, 5))
        
        ctk.CTkLabel(
            self.card_forms, text="Preencha os dados da loja que irá receber os dados migrados.", 
            font=("Segoe UI", 13), text_color=TEXT_MUTED, anchor="w"
        ).pack(fill="x", padx=25, pady=(0, 20))

        # Campo: Empresa Origem
        self._criar_input_label(self.card_forms, "EMPRESA DE ORIGEM")
        opt = ctk.CTkOptionMenu(
            self.card_forms, values=["EXINIA CONFECCOES E COMERCIO", "OUTRA FILIAL LTDA"],
            height=45, fg_color=BG_COLOR, button_color=PRIMARY, font=("Segoe UI", 14)
        )
        opt.pack(fill="x", padx=25, pady=(0, 20))

        # Campo: ID Loja Destino
        self._criar_input_label(self.card_forms, "ID DA LOJA NO DESTINO")
        entry = ctk.CTkEntry(
            self.card_forms, placeholder_text="Ex: 1", height=45, 
            fg_color=BG_COLOR, border_color=BORDER_COLOR, font=("Segoe UI", 14)
        )
        entry.pack(fill="x", padx=25, pady=(0, 30))

        # Separador Fino
        ctk.CTkFrame(self.card_forms, height=1, fg_color=BORDER_COLOR).pack(fill="x", pady=10)

        # Título da Área de Ação
        ctk.CTkLabel(
            self.card_forms, text="Painel de Execução", 
            font=("Segoe UI", 15, "bold"), text_color=TEXT_WHITE, anchor="w"
        ).pack(fill="x", padx=25, pady=(15, 10))

        # Área de Ação Multi-botões (dentro do final do card)
        f_action = ctk.CTkFrame(self.card_forms, fg_color="transparent")
        f_action.pack(fill="x", padx=15, pady=(0, 20))
        
        f_action.grid_columnconfigure((0, 1, 2), weight=1)

        btn_cliente = ctk.CTkButton(
            f_action, text="👥 Migrar Clientes", height=55,
            font=("Segoe UI", 14, "bold"), corner_radius=8,
            fg_color=PRIMARY, hover_color=PRIMARY_HOVER
        )
        btn_cliente.grid(row=0, column=0, sticky="ew", padx=10)

        btn_venda = ctk.CTkButton(
            f_action, text="🛒 Migrar Vendas", height=55,
            font=("Segoe UI", 14, "bold"), corner_radius=8,
            fg_color=SUCCESS, hover_color="#059669" # Verde mais escuro no hover
        )
        btn_venda.grid(row=0, column=1, sticky="ew", padx=10)

        # Container do Truncate (Botão + Checkbox)
        f_truncate = ctk.CTkFrame(f_action, fg_color="transparent")
        f_truncate.grid(row=0, column=2, sticky="ew", padx=10)
        
        btn_truncate = ctk.CTkButton(
            f_truncate, text="❌ Limpar Destino", height=55,
            font=("Segoe UI", 14, "bold"), corner_radius=8,
            fg_color="#DC2626", hover_color="#B91C1C" # Tons de alerta (Vermelho)
        )
        btn_truncate.pack(fill="x")
        
        chk_truncate = ctk.CTkCheckBox(
            f_truncate, text="Autorizar Truncate", font=("Segoe UI", 12),
            text_color=TEXT_MUTED, checkbox_width=20, checkbox_height=20
        )
        chk_truncate.pack(pady=(8,0))

    def _criar_metrica_card(self, parent, col, titulo, valor, subtitulo):
        card = ctk.CTkFrame(
            parent, fg_color=CARD_COLOR, corner_radius=12,
            border_width=1, border_color=BORDER_COLOR, height=120
        )
        card.grid(row=0, column=col, sticky="ew", padx=10)
        card.pack_propagate(False)

        ctk.CTkLabel(card, text=titulo, font=("Segoe UI", 13, "bold"), text_color=TEXT_MUTED, anchor="w").pack(fill="x", padx=20, pady=(20, 5))
        ctk.CTkLabel(card, text=valor, font=("Segoe UI", 28, "bold"), text_color=TEXT_WHITE, anchor="w").pack(fill="x", padx=20)
        ctk.CTkLabel(card, text=subtitulo, font=("Segoe UI", 12), text_color=PRIMARY, anchor="w").pack(fill="x", padx=20, pady=(5, 0))

    def _criar_input_label(self, parent, texto):
        ctk.CTkLabel(parent, text=texto, font=("Segoe UI", 11, "bold"), text_color=TEXT_MUTED, anchor="w").pack(fill="x", padx=25, pady=(0, 5))

if __name__ == "__main__":
    app = WebStyleMockup()
    app.mainloop()
