import duckdb
import pandas as pd
import matplotlib.pyplot as plt

# Caminho do seu banco de dados
DB_PATH = '/home/aidea/data_backtest/backtest_8months.db'

def plot_cluster_validation():
    print(f"[*] Conectando ao DuckDB: {DB_PATH}")
    con = duckdb.connect(DB_PATH, read_only=True)
    
    # Extrai 150 clusters sequenciais (ignorando os primeiros para fugir do ruído da abertura)
    query = """
        SELECT 
            timestamp_ns, 
            total_bid, 
            total_ask, 
            cumdelta, 
            deltamin, 
            deltamax
        FROM liquidity_clusters 
        ORDER BY timestamp_ns
        LIMIT 150 OFFSET 1000
    """
    
    try:
        df = con.execute(query).df()
    except Exception as e:
        print(f"[-] Erro ao ler a tabela: {e}")
        return

    if df.empty:
        print("[-] Nenhum dado retornado. Verifique o nome da tabela.")
        return

    # Converte timestamp para um formato legível (assumindo nanosegundos)
    df['datetime'] = pd.to_datetime(df['timestamp_ns'], unit='ns')
    df.set_index('datetime', inplace=True)

    print(f"[*] Gerando gráfico para {len(df)} clusters...")

    # Configuração do painel gráfico (Alta resolução)
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 8), gridspec_kw={'height_ratios': [2, 1]}, sharex=True)
    fig.suptitle('Validação Matemática: Order Flow Clusters', fontsize=16, fontweight='bold')

    # ==========================================
    # PAINEL 1: CVD e Excursão de Delta
    # ==========================================
    ax1.plot(df.index, df['cumdelta'], color='blue', linewidth=2, label='CVD (Fechamento do Cluster)')
    
    # Preenche a área entre Deltamin e Deltamax para mostrar a volatilidade interna do fluxo
    ax1.fill_between(df.index, df['deltamin'], df['deltamax'], color='lightblue', alpha=0.5, label='Excursão (Deltamin / Deltamax)')
    
    ax1.axhline(0, color='black', linewidth=1, linestyle='--')
    ax1.set_ylabel('Delta Acumulado')
    ax1.set_title('Cumulative Volume Delta (CVD) e Amplitude Interna')
    ax1.legend(loc='upper left')
    ax1.grid(True, linestyle=':', alpha=0.6)

    # ==========================================
    # PAINEL 2: Volume Bruto (Agressões)
    # ==========================================
    width = (df.index[1] - df.index[0]) * 0.8 if len(df) > 1 else 1 # Largura dinâmica das barras
    
    ax2.bar(df.index, df['total_bid'], width=width, color='green', alpha=0.7, label='Agressão Compra (Bid)')
    # Inverte o volume de Ask para desenhar para baixo (formato clássico de footprint/volume)
    ax2.bar(df.index, -df['total_ask'], width=width, color='red', alpha=0.7, label='Agressão Venda (Ask)')
    
    ax2.set_ylabel('Volume')
    ax2.set_title('Volume Direcional Bruto')
    ax2.legend(loc='upper left')
    ax2.grid(True, linestyle=':', alpha=0.6)

    plt.xticks(rotation=45)
    plt.tight_layout()

    # Salva no disco ao invés de tentar abrir janela no WSL
    output_file = 'validation_plot.png'
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"[+] Gráfico salvo com sucesso: {output_file}")

if __name__ == "__main__":
    plot_cluster_validation()
