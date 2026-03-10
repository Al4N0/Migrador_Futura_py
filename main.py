import os
import customtkinter as ctk
from dotenv import load_dotenv

# Importar nossas classes de banco de dados recém-criadas
from core import ConexaoFirebird, ConexaoMySQL

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

class AppMigrador(ctk.CTk):
    def __init__(self):
        super().__init__()

        # =========================================================
        # 1. Configuração da Janela Principal
        # =========================================================
        self.title("Migrador: Firebird 3 ➔ MySQL 8")
        self.geometry("900x650")
        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("blue")

        # 2. Configurando o layout principal (Grid)
        # O grid nos ajuda a dividir a tela em duas colunas proporcionais
        self.grid_columnconfigure(0, weight=1) # Coluna da Esquerda (Configurações)
        self.grid_columnconfigure(1, weight=2) # Coluna da Direita (Status e Logs)
        self.grid_rowconfigure(0, weight=1)

        # =========================================================
        # FRAME ESQUERDO: Configurações de Banco de Dados e Ações
        # =========================================================
        self.frame_esq = ctk.CTkFrame(self)
        self.frame_esq.grid(row=0, column=0, padx=10, pady=10, sticky="nsew")

        # --- Seção Firebird ---
        self.lbl_fb_title = ctk.CTkLabel(self.frame_esq, text="🔥 Conexão Firebird 3", font=("", 16, "bold"))
        self.lbl_fb_title.pack(pady=(10, 5), padx=10, anchor="w")

        self.entry_fb_path = ctk.CTkEntry(self.frame_esq, placeholder_text="Caminho do Banco (.fdb ou .ibl)")
        self.entry_fb_path.pack(fill="x", padx=10, pady=5)
        # Tenta carregar do .env, se não existir deixa vazio
        if os.getenv("FB_PATH"):
            self.entry_fb_path.insert(0, os.getenv("FB_PATH"))

        self.entry_fb_user = ctk.CTkEntry(self.frame_esq, placeholder_text="Usuário (ex: SYSDBA)")
        self.entry_fb_user.pack(fill="x", padx=10, pady=5)
        self.entry_fb_user.insert(0, os.getenv("FB_USER", "SYSDBA"))

        self.entry_fb_pass = ctk.CTkEntry(self.frame_esq, placeholder_text="Senha", show="*")
        self.entry_fb_pass.pack(fill="x", padx=10, pady=5)
        self.entry_fb_pass.insert(0, os.getenv("FB_PASS", "masterkey"))

        # --- Seção MySQL ---
        self.lbl_my_title = ctk.CTkLabel(self.frame_esq, text="🐬 Conexão MySQL 8", font=("", 16, "bold"))
        self.lbl_my_title.pack(pady=(20, 5), padx=10, anchor="w")

        self.entry_my_host = ctk.CTkEntry(self.frame_esq, placeholder_text="Host (ex: localhost)")
        self.entry_my_host.pack(fill="x", padx=10, pady=5)
        self.entry_my_host.insert(0, os.getenv("MYSQL_HOST", "localhost"))

        self.entry_my_db = ctk.CTkEntry(self.frame_esq, placeholder_text="Nome do Banco de Dados")
        self.entry_my_db.pack(fill="x", padx=10, pady=5)
        if os.getenv("MYSQL_DB"):
            self.entry_my_db.insert(0, os.getenv("MYSQL_DB"))

        self.entry_my_user = ctk.CTkEntry(self.frame_esq, placeholder_text="Usuário (ex: root)")
        self.entry_my_user.pack(fill="x", padx=10, pady=5)
        self.entry_my_user.insert(0, os.getenv("MYSQL_USER", "root"))

        self.entry_my_pass = ctk.CTkEntry(self.frame_esq, placeholder_text="Senha", show="*")
        self.entry_my_pass.pack(fill="x", padx=10, pady=5)
        if os.getenv("MYSQL_PASS"):
             self.entry_my_pass.insert(0, os.getenv("MYSQL_PASS"))

        # --- Botões de Ação ---
        # Botão para testar configurações de conexão sem migrar
        self.btn_test_conn = ctk.CTkButton(self.frame_esq, text="🔌 Testar Conexões", command=self.teste_conexoes)
        self.btn_test_conn.pack(fill="x", padx=10, pady=(30, 10))

        # Botão principal que só será habilitado após conexões com sucesso (no Módulo 3/4)
        self.btn_migrar = ctk.CTkButton(self.frame_esq, text="🚀 Iniciar Migração", fg_color="green", hover_color="darkgreen", command=self.iniciar_migracao, state="disabled")
        self.btn_migrar.pack(fill="x", padx=10, pady=10)


        # =========================================================
        # FRAME DIREITO: Logs Gerais e Progresso
        # =========================================================
        self.frame_dir = ctk.CTkFrame(self)
        self.frame_dir.grid(row=0, column=1, padx=(0, 10), pady=10, sticky="nsew")

        self.lbl_log_title = ctk.CTkLabel(self.frame_dir, text="📋 Progresso e Logs da Migração", font=("", 16, "bold"))
        self.lbl_log_title.pack(pady=(10, 5), anchor="w", padx=10)

        # Barra de progresso para a cópia dos dados
        self.progress_bar = ctk.CTkProgressBar(self.frame_dir)
        self.progress_bar.pack(fill="x", padx=10, pady=(5, 15))
        self.progress_bar.set(0) # Inicia em 0%

        # Caixa de texto para exibir erros, alertas e sucesso ao usuário
        self.txt_log = ctk.CTkTextbox(self.frame_dir, font=("Consolas", 12))
        self.txt_log.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        
        # Mensagem inicial ao abrir
        self.log_message("Sistema iniciado. Aguardando configurações...\n")

    # =============================================================
    # Métodos Auxiliares e Regras de Negócio
    # =============================================================
    def log_message(self, mensagem):
        """Método para facilitar a vida: escreve no final do log e rola a tela."""
        self.txt_log.insert("end", mensagem + "\n")
        self.txt_log.see("end") 

    def teste_conexoes(self):
        """Ação de clique do botão: Testar Conexões"""
        self.log_message("\n> [Iniciando] Testando Conexões...")
        
        # 1. Testar Firebird
        path_fb = self.entry_fb_path.get()
        user_fb = self.entry_fb_user.get()
        pass_fb = self.entry_fb_pass.get()

        if not path_fb:
            self.log_message("⚠️ Caminho do Firebird está vazio.")
            return

        fb = ConexaoFirebird(path_fb, user_fb, pass_fb)
        sucesso_fb, msg_fb = fb.conectar()
        
        if sucesso_fb:
            self.log_message(f"✅ {msg_fb}")
            fb.desconectar()
        else:
            self.log_message(f"❌ {msg_fb}")
            return # Aborta se falhar

        # 2. Testar MySQL
        host_my = self.entry_my_host.get()
        user_my = self.entry_my_user.get()
        pass_my = self.entry_my_pass.get()
        db_my = self.entry_my_db.get()

        if not db_my:
            self.log_message("⚠️ Nome do bando de dados MySQL está vazio.")
            return

        my = ConexaoMySQL(host_my, user_my, pass_my, db_my)
        sucesso_my, msg_my = my.conectar()

        if sucesso_my:
            self.log_message(f"✅ {msg_my}")
            my.desconectar()
        else:
            self.log_message(f"❌ {msg_my}")
            return # Aborta se falhar

        # Se ambos conectaram, libera o botão principal de migrar!
        self.log_message("🎉 Sucesso! Ambos os bancos estão comunicando perfeitamente.")
        self.btn_migrar.configure(state="normal")
        
    def iniciar_migracao(self):
        self.log_message("\n[!!!] Botão Migrar Clicado (Módulo 4 ensinará a extração em lote)")

if __name__ == "__main__":
    app = AppMigrador()
    app.mainloop()
