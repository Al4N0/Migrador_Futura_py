import os
import customtkinter as ctk
from dotenv import load_dotenv

# Importar nossas classes de banco de dados recém-criadas
from core import ConexaoFirebird, ConexaoMySQL

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

class JanelaConfiguracoes(ctk.CTkToplevel):
    # Recebemos 'parent' para sabermos quem é nossa tela principal
    def __init__(self, parent, *args, **kwargs):
        super().__init__(parent, *args, **kwargs)
        self.parent = parent  # Vamos guardar isso para jogar os logs lá na tela principal
        self.title("Configurações do Banco de Dados")
        # Definimos o tamanho desejado para o Pop-Up
        largura_janela = 450
        altura_janela = 550
        # Pegamos a resolução do Monitor
        largura_tela = self.winfo_screenwidth()
        altura_tela = self.winfo_screenheight()
        # Calculamos as posições X e Y
        pos_x = int((largura_tela / 2) - (largura_janela / 2))
        pos_y = int((altura_tela / 2) - (altura_janela / 2))
        # Passamos a nova string geometry: "LARGURAxALTURA+EIXO_X+EIXO_Y"
        self.geometry(f"{largura_janela}x{altura_janela}+{pos_x}+{pos_y}")

        # Comportamento modal (trava a tela de trás)
        self.grab_set()

        # =========================================================
        # FRAME ÚNICO: Configurações na janela separada
        # =========================================================
        # CTkScrollableFrame permite rolagem caso a tela seja pequena
        self.frame_config = ctk.CTkScrollableFrame(self)
        self.frame_config.pack(fill="both", expand=True, padx=10, pady=10)

        # --- Seção Firebird ---
        self.lbl_fb_title = ctk.CTkLabel(self.frame_config, text="🔥 Conexão Firebird 3", font=("", 16, "bold"))
        self.lbl_fb_title.pack(pady=(10, 5), padx=10, anchor="w")

        self.entry_fb_path = ctk.CTkEntry(self.frame_config, placeholder_text="Caminho do Banco (.fdb ou .ibl)")
        self.entry_fb_path.pack(fill="x", padx=10, pady=5)
        if os.getenv("FB_PATH"):
            self.entry_fb_path.insert(0, os.getenv("FB_PATH"))

        self.entry_fb_user = ctk.CTkEntry(self.frame_config, placeholder_text="Usuário (ex: SYSDBA)")
        self.entry_fb_user.pack(fill="x", padx=10, pady=5)
        self.entry_fb_user.insert(0, os.getenv("FB_USER", "SYSDBA"))

        self.entry_fb_pass = ctk.CTkEntry(self.frame_config, placeholder_text="Senha", show="*")
        self.entry_fb_pass.pack(fill="x", padx=10, pady=5)
        self.entry_fb_pass.insert(0, os.getenv("FB_PASS", "masterkey"))

        # --- Seção MySQL ---
        self.lbl_my_title = ctk.CTkLabel(self.frame_config, text="🐬 Conexão MySQL 8", font=("", 16, "bold"))
        self.lbl_my_title.pack(pady=(20, 5), padx=10, anchor="w")

        self.entry_my_host = ctk.CTkEntry(self.frame_config, placeholder_text="Host (ex: localhost)")
        self.entry_my_host.pack(fill="x", padx=10, pady=5)
        self.entry_my_host.insert(0, os.getenv("MYSQL_HOST", "localhost"))

        self.entry_my_db = ctk.CTkEntry(self.frame_config, placeholder_text="Nome do Banco de Dados")
        self.entry_my_db.pack(fill="x", padx=10, pady=5)
        if os.getenv("MYSQL_DB"):
            self.entry_my_db.insert(0, os.getenv("MYSQL_DB"))

        self.entry_my_user = ctk.CTkEntry(self.frame_config, placeholder_text="Usuário (ex: root)")
        self.entry_my_user.pack(fill="x", padx=10, pady=5)
        self.entry_my_user.insert(0, os.getenv("MYSQL_USER", "root"))

        self.entry_my_pass = ctk.CTkEntry(self.frame_config, placeholder_text="Senha", show="*")
        self.entry_my_pass.pack(fill="x", padx=10, pady=5)
        if os.getenv("MYSQL_PASS"):
             self.entry_my_pass.insert(0, os.getenv("MYSQL_PASS"))
             
        # O botão Testar Conexões vem pra cá!
        self.btn_test_conn = ctk.CTkButton(self.frame_config, text="🔌 Testar Conexões", command=self.teste_conexoes)
        self.btn_test_conn.pack(fill="x", padx=10, pady=(30, 20))

    def teste_conexoes(self):
        """A ação de testar agora pertence à janela de configurações"""
        # Perceba que usamos "self.parent.log_message" para imprimir na tela preta que está atrás!
        self.parent.log_message("\n> [Iniciando] Testando Conexões...")
        
        path_fb = self.entry_fb_path.get()
        user_fb = self.entry_fb_user.get()
        pass_fb = self.entry_fb_pass.get()

        if not path_fb:
            self.parent.log_message("⚠️ Caminho do Firebird está vazio.")
            return

        fb = ConexaoFirebird(path_fb, user_fb, pass_fb)
        sucesso_fb, msg_fb = fb.conectar()
        
        if sucesso_fb:
            self.parent.log_message(f"✅ {msg_fb}")
            fb.desconectar()
        else:
            self.parent.log_message(f"❌ {msg_fb}")
            return # Aborta

        host_my = self.entry_my_host.get()
        user_my = self.entry_my_user.get()
        pass_my = self.entry_my_pass.get()
        db_my = self.entry_my_db.get()

        if not db_my:
            self.parent.log_message("⚠️ Nome do bando de dados MySQL está vazio.")
            return

        my = ConexaoMySQL(host_my, user_my, pass_my, db_my)
        sucesso_my, msg_my = my.conectar()

        if sucesso_my:
            self.parent.log_message(f"✅ {msg_my}")
            my.desconectar()
        else:
            self.parent.log_message(f"❌ {msg_my}")
            return 

        self.parent.log_message("🎉 Sucesso! Ambos os bancos estão comunicando perfeitamente.")
        # Libera o botão "🚀 Iniciar Migração" que está na janela pai!
        self.parent.btn_migrar.configure(state="normal")
        # Fecha essa janela filha
        self.destroy() 


class AppMigrador(ctk.CTk):
    def __init__(self):
        super().__init__()

        self.title("Migrador Futura: Firebird 3 ➔ MySQL 8")
        
        # --- Lógica de Centralização (Tela Principal) ---
        largura_janela = 900
        altura_janela = 650
        largura_tela = self.winfo_screenwidth()
        altura_tela = self.winfo_screenheight()
        pos_x = int((largura_tela / 2) - (largura_janela / 2))
        pos_y = int((altura_tela / 2) - (altura_janela / 2))
        self.geometry(f"{largura_janela}x{altura_janela}+{pos_x}+{pos_y}")
        # ------------------------------------------------
        
        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("blue")

        self.grid_columnconfigure(0, weight=1) 
        self.grid_columnconfigure(1, weight=2) 
        self.grid_rowconfigure(0, weight=1)              

        # =========================================================
        # FRAME ESQUERDO: Ações Limpas e Simplificadas
        # =========================================================
        self.frame_esq = ctk.CTkFrame(self)
        self.frame_esq.grid(row=0, column=0, padx=10, pady=10, sticky="nsew")

        # Olha o botão da engrenagem aqui limpinho!
        self.btn_config = ctk.CTkButton(self.frame_esq, text="⚙️ Configurar Bancos", command=self.abrir_configuracoes)
        self.btn_config.pack(fill="x", padx=10, pady=(30, 10))

        # Botão principal que só será habilitado após conexões com sucesso
        self.btn_migrar = ctk.CTkButton(self.frame_esq, text="🚀 Iniciar Migração", fg_color="green", hover_color="darkgreen", command=self.iniciar_migracao, state="disabled")
        self.btn_migrar.pack(fill="x", padx=10, pady=10)


        # =========================================================
        # FRAME DIREITO: Logs Gerais e Progresso
        # =========================================================
        self.frame_dir = ctk.CTkFrame(self)
        self.frame_dir.grid(row=0, column=1, padx=(0, 10), pady=10, sticky="nsew")

        self.lbl_log_title = ctk.CTkLabel(self.frame_dir, text="📋 Progresso e Logs da Migração", font=("", 16, "bold"))
        self.lbl_log_title.pack(pady=(10, 5), anchor="w", padx=10)

        self.progress_bar = ctk.CTkProgressBar(self.frame_dir)
        self.progress_bar.pack(fill="x", padx=10, pady=(5, 15))
        self.progress_bar.set(0) 

        self.txt_log = ctk.CTkTextbox(self.frame_dir, font=("Consolas", 12))
        self.txt_log.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        
        self.log_message("Sistema iniciado. Clique em 'Configurar Bancos'.\n")

    def log_message(self, mensagem):
        self.txt_log.insert("end", mensagem + "\n")
        self.txt_log.see("end") 

    def iniciar_migracao(self):
        self.log_message("\n[!!!] Botão Migrar Clicado (Módulo 4 ensinará a extração em lote)")

    def abrir_configuracoes(self):
        # Aqui, instanciamos a Janela de Configurações, passando `self` (A Aplicação principal)
        # como parâmetro. Assim a Janela Filha consegue falar com a Mãe.
        janela = JanelaConfiguracoes(self)    


if __name__ == "__main__":
    app = AppMigrador()
    app.mainloop()
