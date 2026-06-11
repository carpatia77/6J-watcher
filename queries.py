def build_mfe_mae_cte(
    symbol: str = '6J',
    start_date: str = None,
    end_date: str = None,
    horizon_minutes: int = 2,
    horizon_spoofing_minutes: int = 2,
    is_sampling: bool = False,
    sample_size: int = 20000,
    filter_dates: list = None
) -> str:
    """
    Constrói apenas a parte da CTE (até 'scored') para cálculo direcional de MFE e MAE.
    Pode ser estendida pelo profiler com GROUP BYs customizados.
    """
    date_filter = ""
    if start_date and end_date:
        date_filter = f"AND timestamp >= '{start_date}' AND timestamp < '{end_date}'"
    elif start_date:
        date_filter = f"AND timestamp >= '{start_date}'"
    elif end_date:
        date_filter = f"AND timestamp < '{end_date}'"

    if filter_dates:
        dates_str = ",".join([f"'{str(d).replace(chr(39), '')}' " for d in filter_dates])
        date_filter += f" AND CAST(timestamp AS DATE) IN ({dates_str})"

    sampling_cte = f"SELECT * FROM full_clusters USING SAMPLE RESERVOIR({sample_size} ROWS)" if is_sampling else "SELECT * FROM full_clusters"
    
    # 1 minuto = 60000000000 ns
    horizon_ns = int(horizon_minutes * 60_000_000_000)
    horizon_spoofing_ns = int(horizon_spoofing_minutes * 60_000_000_000)

    return f"""
    WITH full_clusters AS (
        SELECT *,
            (AVG(price) OVER (
                PARTITION BY symbol 
                ORDER BY timestamp_ns 
                ROWS BETWEEN 2001 PRECEDING AND 1 PRECEDING)
            - AVG(price) OVER (
                PARTITION BY symbol 
                ORDER BY timestamp_ns 
                ROWS BETWEEN 4001 PRECEDING AND 2002 PRECEDING)
            ) AS price_slope_4h
        FROM liquidity_clusters
        WHERE symbol = '{symbol}' 
          {date_filter}
    ),
    sampled AS (
        {sampling_cte}
    ),
    cluster_excursions AS (
        SELECT
            c.timestamp,
            c.timestamp_ns,
            c.behavior_signature,
            c.session,
            (c.total_bid + c.total_ask)            AS total_vol,
            c.total_bid,
            c.total_ask,
            c.cumdelta,
            ABS(c.total_bid - c.total_ask)         AS imbalance,
            c.price                                AS c_price,
            COALESCE(MAX(t.price), c.price)        AS max_future_price,
            COALESCE(MIN(t.price), c.price)        AS min_future_price,
            CASE 
                WHEN ABS(c.price_slope_4h) > 0.000010 THEN 'TRENDING'
                ELSE 'RANGING'
            END AS regime
        FROM sampled c
        LEFT JOIN tape_events t
            ON  c.symbol = t.symbol
            AND (
                CASE WHEN c.timestamp_ns IS NOT NULL AND t.timestamp_ns IS NOT NULL
                     THEN t.timestamp_ns > c.timestamp_ns
                          AND t.timestamp_ns <= c.timestamp_ns + (CASE WHEN c.behavior_signature = 'spoofing_wall' THEN {horizon_spoofing_ns} ELSE {horizon_ns} END)
                     ELSE t.timestamp > c.timestamp
                          AND t.timestamp <= c.timestamp + INTERVAL 1 MINUTE * (CASE WHEN c.behavior_signature = 'spoofing_wall' THEN {horizon_spoofing_minutes} ELSE {horizon_minutes} END)
                END
            )
        GROUP BY c.timestamp, c.timestamp_ns, c.behavior_signature, c.session, c.price, c.price_slope_4h, c.total_bid, c.total_ask, c.cumdelta
    ),
    mfe_mae_calc AS (
        SELECT *,
            CASE
                WHEN behavior_signature IN ('iceberg_accumulation', 'breakout_genuine', 'defense_line') THEN max_future_price - c_price
                WHEN behavior_signature IN ('iceberg_distribution', 'absorption_passive') THEN c_price - min_future_price
                WHEN behavior_signature IN ('spoofing_wall', 'liquidity_vacuum') AND total_bid > total_ask THEN max_future_price - c_price
                WHEN behavior_signature IN ('spoofing_wall', 'liquidity_vacuum') AND total_ask > total_bid THEN c_price - min_future_price
                ELSE GREATEST(max_future_price - c_price, c_price - min_future_price)
            END AS mfe,
            CASE
                WHEN behavior_signature IN ('iceberg_accumulation', 'breakout_genuine', 'defense_line') THEN c_price - min_future_price
                WHEN behavior_signature IN ('iceberg_distribution', 'absorption_passive') THEN max_future_price - c_price
                WHEN behavior_signature IN ('spoofing_wall', 'liquidity_vacuum') AND total_bid > total_ask THEN c_price - min_future_price
                WHEN behavior_signature IN ('spoofing_wall', 'liquidity_vacuum') AND total_ask > total_bid THEN max_future_price - c_price
                ELSE GREATEST(max_future_price - c_price, c_price - min_future_price)
            END AS mae
        FROM cluster_excursions
    ),
    scored AS (
        SELECT *,
            CASE WHEN mfe > mae AND mfe > 0 THEN 1 ELSE 0 END AS win,
            CASE WHEN mfe > mae AND mfe > 0 THEN mfe ELSE 0 END AS gross_profit,
            CASE WHEN mae > mfe THEN mae ELSE 0 END AS gross_loss
        FROM mfe_mae_calc
    )"""

def build_mfe_mae_query(
    symbol: str = '6J',
    start_date: str = None,
    end_date: str = None,
    signature: str = 'absorption_passive',
    session: str = 'LONDON',
    regime: str = 'RANGING',
    horizon_minutes: int = 2,
    horizon_spoofing_minutes: int = 2,
    is_sampling: bool = False,
    sample_size: int = 20000
) -> str:
    """
    Constrói a CTE e a consulta final de validação pontual OOS.
    """
    cte = build_mfe_mae_cte(
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        horizon_minutes=horizon_minutes,
        horizon_spoofing_minutes=horizon_spoofing_minutes,
        is_sampling=is_sampling,
        sample_size=sample_size
    )

    query = cte + """
    SELECT 
        COUNT(*) AS total_samples,
        AVG(win) AS win_rate,
        SUM(gross_profit) / NULLIF(SUM(gross_loss), 0) AS profit_factor,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY mfe) AS mfe_p50,
        PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY mfe) AS mfe_p90,
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY mfe) AS mfe_p99,
        
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY mae) AS mae_p50,
        PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY mae) AS mae_p90,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY mae) AS mae_p95,
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY mae) AS mae_p99
    FROM scored
    WHERE 1=1
    """

    if signature:
        query += f"\n      AND behavior_signature = '{signature}'"
    if session:
        query += f"\n      AND session = '{session}'"
    if regime:
        query += f"\n      AND regime = '{regime}'"

    query += ";"
    
    return query
