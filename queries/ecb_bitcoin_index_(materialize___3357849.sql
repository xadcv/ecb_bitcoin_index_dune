-- part of a query repo
-- query name: ECB Bitcoin Index (Materialized View)
-- query link: https://dune.com/queries/3357849

-- Materialized in dune.adcv.result_ecb_bitcoin_index
-- Testing push from gh

WITH 

priceBTC AS (
    SELECT 
        date_trunc('day', minute) AS period,
        max_by(price, minute) AS price_BTC
    FROM prices.usd
    WHERE symbol = 'BTC' AND contract_address IS NULL
    GROUP BY 1
),

rangeYvesMersch AS ( -- https://x.com/ecb/status/961557125771288576?s=20

    SELECT period
    FROM unnest(sequence(
        CAST('2018-02-18' AS TIMESTAMP), 
        CAST(NOW() as timestamp),
        interval '1' day)
    ) as s(period)
),

rangeBenoitCoeure AS ( -- https://x.com/ecb/status/1063045037813051392?s=20

    SELECT period
    FROM unnest(sequence(
        CAST('2018-11-15' AS TIMESTAMP), 
        CAST(NOW() as timestamp),
        interval '1' day)
    ) as s(period)
),

rangePeterPraet AS ( -- https://x.com/ecb/status/1105489733654851591?s=20

    SELECT period
    FROM unnest(sequence(
        CAST('2019-03-12' AS TIMESTAMP), 
        CAST(NOW() as timestamp),
        interval '1' day)
    ) as s(period)
),

rangeFabioPanetta AS ( -- https://x.com/ecb/status/1469244896741666819?s=20

    SELECT period
    FROM unnest(sequence(
        CAST('2021-12-10' AS TIMESTAMP), 
        CAST(NOW() as timestamp),
        interval '1' day)
    ) as s(period)
),

rangeJuergenSchaff AS ( -- https://x.com/ecb/status/1597894360510922752?s=20
    SELECT period
    FROM unnest(sequence(
        CAST('2022-11-30' AS TIMESTAMP), 
        CAST(NOW() as timestamp),
        interval '1' day)
    ) as s(period)
),

pricesMersch AS (
    SELECT 
        period,
        price_BTC
        --price_ETH
    FROM priceBTC
    INNER JOIN rangeYvesMersch USING (period)
    ORDER BY period ASC
),

pricesCoeure AS (
    SELECT 
        period,
        price_BTC
        --price_ETH
    FROM priceBTC
    INNER JOIN rangeBenoitCoeure USING (period)
    ORDER BY period ASC
),

pricesPraet AS (
    SELECT 
        period,
        price_BTC
        --price_ETH
    FROM priceBTC
    INNER JOIN rangePeterPraet USING (period)
    ORDER BY period ASC
),

pricesPanetta AS (
    SELECT 
        period,
        price_BTC
        --price_ETH
    FROM priceBTC
    INNER JOIN rangeFabioPanetta USING (period)
    ORDER BY period ASC
),

pricesSchaff AS (
    SELECT 
        period,
        price_BTC
        --price_ETH
    FROM priceBTC
    INNER JOIN rangeJuergenSchaff USING (period)
    ORDER BY period ASC
),

initial_shares_Mersch AS (
    SELECT
        period,
        100 / price_BTC AS merschSats
    FROM pricesMersch
    ORDER BY period
    LIMIT 1
),

initial_shares_Coeure AS (
    SELECT
        period,
        100 / price_BTC AS coeureSats
    FROM pricesCoeure
    ORDER BY period
    LIMIT 1
),

initial_shares_Praet AS (
    SELECT
        period,
        100 / price_BTC AS praetSats
    FROM pricesPraet
    ORDER BY period
    LIMIT 1
),

initial_shares_Panetta AS (
    SELECT
        period,
        100 / price_BTC AS panettaSats
    FROM pricesPanetta
    ORDER BY period
    LIMIT 1
),

initial_shares_Schaff AS (
    SELECT
        period,
        100 / price_BTC AS schaffSats
    FROM pricesSchaff
    ORDER BY period
    LIMIT 1
),

constant_val_Mersch AS (
    SELECT 
        period,
        price_BTC,
        price_BTC * FIRST_VALUE(merschSats) OVER (ORDER BY period) AS mersch
    FROM pricesMersch
    LEFT JOIN initial_shares_Mersch USING (period)
),

constant_val_Coeure AS (
    SELECT 
        period,
        price_BTC,
        price_BTC * FIRST_VALUE(coeureSats) OVER (ORDER BY period) AS coeure
    FROM pricesCoeure
    LEFT JOIN initial_shares_Coeure USING (period)
),

constant_val_Praet AS (
    SELECT 
        period,
        price_BTC,
        price_BTC * FIRST_VALUE(praetSats) OVER (ORDER BY period) AS praet
    FROM pricesPraet
    LEFT JOIN initial_shares_Praet USING (period)
),

constant_val_Panetta AS (
    SELECT 
        period,
        price_BTC,
        price_BTC * FIRST_VALUE(panettaSats) OVER (ORDER BY period) AS panetta
    FROM pricesPanetta
    LEFT JOIN initial_shares_Panetta USING (period)
),

constant_val_Schaff AS (
    SELECT 
        period,
        price_BTC,
        price_BTC * FIRST_VALUE(schaffSats) OVER (ORDER BY period) AS schaff
    FROM pricesSchaff
    LEFT JOIN initial_shares_Schaff USING (period)
),

portfolio_value AS (
    SELECT
        period,
        range.price_BTC AS "BTC",
        100 AS flat,
        ym.mersch AS "Yves Mersch",
        bc.coeure AS "Benoit Coeure",
        pp.praet AS "Peter Praet",
        fp.panetta AS "Fabio Panetta",
        js.schaff AS "Juergen Schaff"
    FROM pricesMersch range
    LEFT JOIN constant_val_Mersch ym USING (period)
    LEFT JOIN constant_val_Coeure bc USING (period)
    LEFT JOIN constant_val_Praet pp USING (period)
    LEFT JOIN constant_val_Panetta fp USING (period)
    LEFT JOIN constant_val_Schaff js USING (period)
)

SELECT * FROM portfolio_value
ORDER BY period DESC

























--         %=                                                                    -%         
--         +@%=                                                                -%@*         
--         .@@@%=                                                            -%@@@:         
--          +@@@@%=                                                        -%@@@@+          
--           #@@@@@%=                                                    -%@@@@@#           
--            %@@@@@@%=                                                -%@@@@@@%.           
--            .#@@@@@@@%=                                            -%@@@@@@@%.            
--              *@@@@@@@@%=.                                      .=%@@@@@@@@#              
--               =@@@@@@@@@@#+-:..                          ..:-+#@@@@@@@@@@=               
--                .+@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@*.                
--                  .+%@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@%+.                  
--                     :=#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@#+:                     
--               ....      .-=+********************************+=-.      ....               
--            =#@@@@@@#=                                             .+#@@@@@%*-            
--            -#@@@@@@@@@+==========================================*@@@@@@@@@#:            
--              -#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@#:              
--                -#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@#:                
--                  -#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@#:                  
--                    :::::::::::::+@@@@@@@@@@*-::::::::::::::::::::::::                    
--                               :#@@@@@@@@@*.                                              
--                             :#@@@@@@@@@*.   .+%%%%%%%%%%%%%%%%%*.                        
--                           .#@@@@@@@@@*.   .*@@@@@@@@@@@@@@@@@#:                          
--                            -#@@@@@@*.   .*@@@@@@@@@@@@@@@@@#:                            
--                              -#@@*.   .*@@@@@@@@@@@@@@@@@#:                              
--                                -.   :*@@@@@@@@@@@@@@@@@#:                                
--                                   .*@@@@@@@@@@@@@@@@@#:                                  
--                                    -#@@@@@@@@@@@@@@#:                                    
--                                      -#@@@@@@@@@@#:                                      
--                                        -#@@@@@%*:                                        
--                                           .::.                                           
-- @steakhouse on Dune