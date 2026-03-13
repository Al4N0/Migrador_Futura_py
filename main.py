import os
import customtkinter as ctk
from dotenv import load_dotenv, set_key

import threading
# Importar nossas classes de banco de dados recém-criadas
from core import ConexaoFirebird, ConexaoMySQL
from migrador_clientes import MigradorClientes

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

        # Frame Host e Porta
        self.frame_fb_hp = ctk.CTkFrame(self.frame_config, fg_color="transparent")
        self.frame_fb_hp.pack(fill="x", padx=10, pady=5)

        self.entry_fb_host = ctk.CTkEntry(self.frame_fb_hp, placeholder_text="Host (ex: localhost)")
        self.entry_fb_host.pack(side="left", fill="x", expand=True, padx=(0, 5))
        self.entry_fb_host.insert(0, os.getenv("FB_HOST", "localhost"))

        self.entry_fb_port = ctk.CTkEntry(self.frame_fb_hp, placeholder_text="Porta (ex: 3050)", width=100)
        self.entry_fb_port.pack(side="right")
        self.entry_fb_port.insert(0, os.getenv("FB_PORT", "3050"))

        self.frame_fb_path = ctk.CTkFrame(self.frame_config, fg_color="transparent")
        self.frame_fb_path.pack(fill="x", padx=10, pady=5)

        self.entry_fb_path = ctk.CTkEntry(self.frame_fb_path, placeholder_text="Caminho do Banco (.fdb ou .ibl)")
        self.entry_fb_path.pack(side="left", fill="x", expand=True, padx=(0, 5))

        self.btn_browse_fb = ctk.CTkButton(self.frame_fb_path, text="Procurar...", width=80, command=self.procurar_banco)
        self.btn_browse_fb.pack(side="right")

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

    def procurar_banco(self):
        caminho_arquivo = ctk.filedialog.askopenfilename(
            title="Selecione o banco de dados Firebird",
            filetypes=[("Firebird Database", "*.fdb *.ibl"), ("Todos os arquivos", "*.*")]
        )
        if caminho_arquivo:
            self.entry_fb_path.delete(0, "end")
            self.entry_fb_path.insert(0, caminho_arquivo)
            # Salva o novo caminho no arquivo .env
            env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
            if not os.path.exists(env_path):
                open(env_path, 'a').close()
            set_key(env_path, "FB_PATH", caminho_arquivo)

    def teste_conexoes(self):
        """A ação de testar agora pertence à janela de configurações"""
        # Perceba que usamos "self.parent.log_message" para imprimir na tela preta que está atrás!
        self.parent.log_message("\n> [Iniciando] Testando Conexões...")
        
        path_fb = self.entry_fb_path.get()
        user_fb = self.entry_fb_user.get()
        pass_fb = self.entry_fb_pass.get()
        host_fb = self.entry_fb_host.get()
        port_fb = self.entry_fb_port.get()

        if not path_fb:
            self.parent.log_message("⚠️ Caminho do Firebird está vazio.")
            return

        # Converter a porta para inteiro se existir
        port_fb_int = int(port_fb) if port_fb.isdigit() else 3050

        fb = ConexaoFirebird(path_fb, user_fb, pass_fb, host_fb, port_fb_int)
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
            my.desconectar()
        else:
            self.parent.log_message(f"❌ {msg_my}")
            return 

        # Salva as configurações de conexão testadas (com sucesso) no .env
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

        self.parent.log_message("🎉 Sucesso! Ambos os bancos estão comunicando perfeitamente e as configurações foram salvas.")
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

        # Frame para agrupar botões e opções da migração de Clientes
        self.frame_mig_clientes = ctk.CTkFrame(self.frame_esq)
        self.frame_mig_clientes.pack(fill="x", padx=10, pady=10)

        # Botão principal que só será habilitado após conexões com sucesso
        self.btn_migrar = ctk.CTkButton(self.frame_mig_clientes, text="🚀 Migrar Clientes", fg_color="green", hover_color="darkgreen", command=self.iniciar_migracao, state="disabled")
        self.btn_migrar.pack(fill="x", padx=10, pady=(10, 5))

        # O usuário pediu para o texto não ser clicável, então separamos a checkbox do texto (Label)
        self.frame_chk_trunc = ctk.CTkFrame(self.frame_mig_clientes, fg_color="transparent")
        self.frame_chk_trunc.pack(fill="x", padx=10, pady=(0, 10))

        self.chk_var_truncar = ctk.BooleanVar(value=False)
        self.chk_truncar = ctk.CTkCheckBox(self.frame_chk_trunc, text="", variable=self.chk_var_truncar, width=20)
        self.chk_truncar.pack(side="left")

        self.lbl_chk_truncar = ctk.CTkLabel(self.frame_chk_trunc, text="Limpar tabela antes (Truncate)")
        self.lbl_chk_truncar.pack(side="left", padx=(5, 0))


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
        self.log_message("\n🚀 ===============================================")
        self.log_message("🚀 [Iniciando] Processo de Migração")
        self.log_message("🚀 ===============================================")
        self.btn_migrar.configure(state="disabled")
        
        # Recuperar conexões salvos no .env para re-instanciar (já que foram testadas)
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
        
        truncar_antes = self.chk_var_truncar.get()
        
        # Executar em thread separada para não travar a interface
        def thread_migracao():
            try:
                sucesso = migrador.executar(truncar=truncar_antes)
                if sucesso:
                    self.log_message("✅ Módulo concluído.")
                else:
                    self.log_message("❌ Módulo falhou.")
            finally:
                # Reativa o botão na thread principal
                self.after(0, lambda: self.btn_migrar.configure(state="normal"))
                
        threading.Thread(target=thread_migracao, daemon=True).start()

    def abrir_configuracoes(self):
        # Aqui, instanciamos a Janela de Configurações, passando `self` (A Aplicação principal)
        # como parâmetro. Assim a Janela Filha consegue falar com a Mãe.
        janela = JanelaConfiguracoes(self)    


if __name__ == "__main__":
    app = AppMigrador()
    app.mainloop()
