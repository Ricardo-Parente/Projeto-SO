import json
import threading
import time
import logging
from kernel_SO import SistemaACL, GerenciadorDeRecursos, MonitorDeadlock

def carregar_casos_teste():
    with open('casos_de_teste.json', 'r') as f:
        return json.load(f)

def executar_processo_simulado(dados_proc, gerenciador):
    nome = dados_proc["nome"]
    usuario = dados_proc["usuario"]
    passos = dados_proc["passos"]
    
    logging.info(f"Processo '{nome}' INICIADO pelo usuario '{usuario}'.")
    
    for passo in passos:
        # Se o processo foi abortado pelo monitor, ele para de executar
        with gerenciador.lock_so:
            if nome not in gerenciador.alocados and passo != passos[0]:
                logging.error(f"Processo '{nome}' finalizado prematuramente (Foi vitima de Deadlock).")
                return

        acao = passo["acao"]
        if acao == "bloquear":
            sucesso = gerenciador.solicitar_bloqueio(nome, usuario, passo["arquivo"], passo["modo"])
            if not sucesso:
                gerenciador.liberar_todos(nome)
                logging.error(f"Processo '{nome}' falhou em adquirir '{passo['arquivo']}'. Encerrando.")
                return
        
        elif acao == "processar":
            time.sleep(passo["tempo"])
            
        elif acao == "liberar":
            gerenciador.liberar_arquivo(nome, passo["arquivo"])

    gerenciador.liberar_todos(nome)
    logging.info(f"Processo '{nome}' CONCLUIDO com sucesso.")

def rodar_cenario(nome_cenario, config_cenario):
    print(f"\n{'='*60}")
    print(f" INICIANDO CENARIO: {nome_cenario}")
    print(f" Descricao: {config_cenario['descricao']}")
    print(f"{'='*60}\n")

    # Boot do Sistema Simulado
    acl = SistemaACL(config_cenario["acl"])
    gerenciador = GerenciadorDeRecursos(acl)
    monitor = MonitorDeadlock(gerenciador)
    
    monitor.start()

    # Criação e inicialização de processos
    threads = []
    for p_data in config_cenario["processos"]:
        t = threading.Thread(target=executar_processo_simulado, args=(p_data, gerenciador))
        threads.append(t)
        t.start()

    # Aguarda processos finalizarem
    for t in threads:
        t.join()

    # Desliga o SO
    monitor.ativo = False
    monitor.join()

    # Saída / Relatório de Métricas
    print(f"\n--- RELATORIO DO CENARIO '{nome_cenario}' ---")
    print(f" Total de Deadlocks Detectados e Resolvidos: {gerenciador.metricas['deadlocks_resolvidos']}")
    print(f" Acessos Negados pela ACL: {gerenciador.metricas['acessos_negados']}")
    print(f" Tempo Total de Espera dos Processos: {gerenciador.metricas['tempo_espera_total']:.4f} segundos")
    print(f"{'='*60}\n")
    time.sleep(1) # Pausa para leitura antes do proximo cenario

if __name__ == "__main__":
    testes = carregar_casos_teste()
    
    # Executa todos os cenários definidos no JSON
    for nome, config in testes.items():
        rodar_cenario(nome, config)