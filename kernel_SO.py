import threading
import time
import logging

# Configuração de log para simular a saída de um terminal de SO
logging.basicConfig(level=logging.INFO, format='[%(relativeCreated)05d ms] [%(levelname)s] %(message)s')

class SistemaACL:
    def __init__(self, regras_iniciais):
        self.regras = regras_iniciais

    def validar(self, usuario, arquivo, modo):
        permissoes = self.regras.get(arquivo, {}).get(usuario, [])
        return modo in permissoes

class GerenciadorDeRecursos:
    def __init__(self, acl):
        self.acl = acl
        self.locks = {}          # arquivo -> threading.Lock()
        self.alocados = {}       # processo -> set(arquivos)
        self.esperando = {}      # processo -> arquivo (O que está travando o processo)
        self.lock_so = threading.Lock() # Protege as estruturas de dados do próprio SO
        self.metricas = {"acessos_negados": 0, "deadlocks_resolvidos": 0, "tempo_espera_total": 0}

    def registrar_processo(self, nome_processo):
        with self.lock_so:
            if nome_processo not in self.alocados:
                self.alocados[nome_processo] = set()
                self.esperando[nome_processo] = None

    def solicitar_bloqueio(self, processo, usuario, arquivo, modo):
        self.registrar_processo(processo)
        
        # 1. Verificação de Segurança (ACL)
        if not self.acl.validar(usuario, arquivo, modo):
            logging.warning(f"ACL NEGOU acesso de '{processo}' ({usuario}) ao arquivo '{arquivo}' (Modo: {modo}).")
            with self.lock_so: self.metricas["acessos_negados"] += 1
            return False

        # 2. Prepara o Lock Físico
        with self.lock_so:
            if arquivo not in self.locks:
                self.locks[arquivo] = threading.Lock()
            self.esperando[processo] = arquivo

        logging.info(f"Processo '{processo}' solicitou '{arquivo}'. Aguardando disponibilidade...")
        inicio = time.time()
        
        # 3. Tenta adquirir o lock. Se demorar, o monitor de deadlock vai agir.
        adquirido = False
        while not adquirido:
            # Tenta bloquear a cada 0.2s para permitir interrupção caso seja escolhido como vítima de deadlock
            adquirido = self.locks[arquivo].acquire(timeout=0.2)
            
            with self.lock_so:
                # Se o processo foi removido das estruturas, ele foi abortado pelo SO
                if processo not in self.alocados:
                    logging.error(f"Processo '{processo}' foi ABORTADO pelo SO enquanto esperava por '{arquivo}'.")
                    return False

        espera = time.time() - inicio
        
        # 4. Lock adquirido com sucesso
        with self.lock_so:
            self.metricas["tempo_espera_total"] += espera
            self.esperando[processo] = None
            self.alocados[processo].add(arquivo)
            
        logging.info(f"Processo '{processo}' BLOQUEOU o arquivo '{arquivo}'. (Espera: {espera:.3f}s)")
        return True

    def liberar_arquivo(self, processo, arquivo):
        with self.lock_so:
            if processo in self.alocados and arquivo in self.alocados[processo]:
                self.locks[arquivo].release()
                self.alocados[processo].remove(arquivo)
                logging.info(f"Processo '{processo}' LIBEROU '{arquivo}'.")

    def liberar_todos(self, processo):
        with self.lock_so:
            if processo in self.alocados:
                for arq in list(self.alocados[processo]):
                    self.locks[arq].release()
                del self.alocados[processo]
                del self.esperando[processo]
                logging.info(f"Recursos do processo '{processo}' foram totalmente limpos.")

class MonitorDeadlock(threading.Thread):
    def __init__(self, gerenciador):
        super().__init__(daemon=True)
        self.gerenciador = gerenciador
        self.ativo = True

    def run(self):
        while self.ativo:
            time.sleep(1.0) # Varredura periódica do SO
            self.analisar_e_resolver()

    def analisar_e_resolver(self):
        with self.gerenciador.lock_so:
            grafo = {}
            dono_do_arquivo = {}
            
            for proc, arquivos in self.gerenciador.alocados.items():
                for arq in arquivos: dono_do_arquivo[arq] = proc

            for proc, arq_desejado in self.gerenciador.esperando.items():
                if arq_desejado and arq_desejado in dono_do_arquivo:
                    grafo[proc] = dono_do_arquivo[arq_desejado]

            # Detecta ciclo
            visitados, pilha = set(), set()
            ciclo_encontrado = []

            def dfs(nodo, caminho_atual):
                visitados.add(nodo)
                pilha.add(nodo)
                caminho_atual.append(nodo)
                
                vizinho = grafo.get(nodo)
                if vizinho:
                    if vizinho not in visitados:
                        if dfs(vizinho, caminho_atual): return True
                    elif vizinho in pilha:
                        # Corta o caminho para mostrar apenas o ciclo
                        idx = caminho_atual.index(vizinho)
                        ciclo_encontrado.extend(caminho_atual[idx:])
                        return True
                
                pilha.remove(nodo)
                caminho_atual.pop()
                return False

            for no in list(grafo.keys()):
                if no not in visitados:
                    if dfs(no, []):
                        self.gerenciador.metricas["deadlocks_resolvidos"] += 1
                        logging.error(f"DEADLOCK DETECTADO! Ciclo: {' -> '.join(ciclo_encontrado)} -> {ciclo_encontrado[0]}")
                        
                        # Resolução: Aborta o último processo do ciclo (vítima)
                        vitima = ciclo_encontrado[-1]
                        logging.warning(f"SO RESOLVENDO: Selecionando '{vitima}' como vitima. Abortando processo...")
                        
                        # Libera recursos da vitima na força
                        arquivos_vitima = list(self.gerenciador.alocados[vitima])
                        for arq in arquivos_vitima:
                            self.gerenciador.locks[arq].release()
                        
                        del self.gerenciador.alocados[vitima]
                        del self.gerenciador.esperando[vitima]
                        break # Resolve um por vez