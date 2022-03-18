DO BEGIN 

    DECLARE minPECLD REAL;
    DECLARE maxPECLD REAL;
    DECLARE maxSCORE REAL;
    DECLARE minSCORE REAL;
    DECLARE maxMTVCOB REAL;
    DECLARE minMTVCOB REAL;

    DECLARE CARTEIRA VARCHAR(255) = ('{carteira}');
    DECLARE DATA_PEDIDO_A DATE  = ('{data_pedido}');
    DECLARE TURMA VARCHAR(255) = ('{turma}');
    
    IF CARTEIRA = 'CONVENCIONAL' AND TURMA = 'STC' THEN
    CREATE LOCAL TEMPORARY TABLE CLB961920."#TAB" AS (
        SELECT  SC.*
        FROM CLB961920.SUSCETIVEIS AS SC
        LEFT JOIN CLB142840.CLUSTER_HIST AS CH ON CH.ZCGACCOUN = SC.ZCGACCOUN AND CH.DATA_PEDIDO = CURRENT_DATE
        LEFT JOIN CLB961920.ATENA_HIST   AS AH ON AH.ZCGACCOUN = SC.ZCGACCOUN AND AH.DATA_PEDIDO = CURRENT_DATE
        WHERE
           ACAO IN ('DISJ','CORTE') AND
           SC.PECLD_CONS > 40 AND
           SC.ZCGMTVCOB > 100 AND           
           DATAREF = CURRENT_DATE AND -- suscetíveis de hoje
           FLAG_SEL = FALSE AND -- ainda não selecionados
           CH.ZCGACCOUN IS NULL AND -- não selecionado (para prevenir update Flag_sel desatualizado)
           AH.ZCGACCOUN IS NULL  -- não selecionado no Atena (ainda não gravado)
 
    );
        
    ELSEIF CARTEIRA = 'CONVENCIONAL' AND TURMA = 'EPS' THEN
    CREATE LOCAL TEMPORARY TABLE CLB961920."#TAB" AS (
        SELECT  SC.*
        FROM CLB961920.SUSCETIVEIS AS SC
        LEFT JOIN CLB142840.CLUSTER_HIST AS CH ON CH.ZCGACCOUN = SC.ZCGACCOUN AND CH.DATA_PEDIDO = CURRENT_DATE
        LEFT JOIN CLB961920.ATENA_HIST   AS AH ON AH.ZCGACCOUN = SC.ZCGACCOUN AND AH.DATA_PEDIDO = CURRENT_DATE
        WHERE
           ACAO IN ('DISJ','CORTE') AND
           SC.PECLD_CONS > 20 AND
           SC.ZCGMTVCOB > 80 AND
           DATAREF = CURRENT_DATE AND -- suscetíveis de hoje
           FLAG_SEL = FALSE AND -- ainda não selecionados
           CH.ZCGACCOUN IS NULL AND -- não selecionado (para prevenir update Flag_sel desatualizado)
           AH.ZCGACCOUN IS NULL  -- não selecionado no Atena (ainda não gravado)
    );
            
    ELSEIF CARTEIRA = 'DISJUNTOR' AND TURMA = 'EPS' THEN
    CREATE LOCAL TEMPORARY TABLE CLB961920."#TAB" AS (
        SELECT  SC.*
        FROM CLB961920.SUSCETIVEIS AS SC
        LEFT JOIN CLB142840.CLUSTER_HIST AS CH ON CH.ZCGACCOUN = SC.ZCGACCOUN AND CH.DATA_PEDIDO = CURRENT_DATE
        LEFT JOIN CLB961920.ATENA_HIST   AS AH ON AH.ZCGACCOUN = SC.ZCGACCOUN AND AH.DATA_PEDIDO = CURRENT_DATE
        LEFT JOIN CLP123432.PLANILHAO_CLIENTE PC ON PC.ZCGACCOUN = SC.ZCGACCOUN    
             JOIN CLB142840.DAG40 AS DAG1 ON PC.ZCGCDMUNI||PC.ZCGCODLOC||PC.ZCGCDBAIR = DAG1.ZCGCDMUNI||DAG1.ZCGCODLOC||DAG1.ZCGCDBAIR    
       AND DAG1.FLAG_GAV = 'S' -- desabilita aqui para liberar seleção fora do mapa    
       WHERE
           ACAO = 'DISJ' AND
           DATAREF = CURRENT_DATE AND -- suscetíveis de hoje
           FLAG_SEL = FALSE AND -- ainda não selecionados
           CH.ZCGACCOUN IS NULL AND -- não selecionado (para prevenir update Flag_sel desatualizado)
           AH.ZCGACCOUN IS NULL  -- não selecionado no Atena (ainda não gravado)
    );
    ELSEIF CARTEIRA = 'DISJUNTOR' AND TURMA = 'STC' THEN
    CREATE LOCAL TEMPORARY TABLE CLB961920."#TAB" AS (
        SELECT  SC.*
        FROM CLB961920.SUSCETIVEIS AS SC
        LEFT JOIN CLB142840.CLUSTER_HIST AS CH ON CH.ZCGACCOUN = SC.ZCGACCOUN AND CH.DATA_PEDIDO = CURRENT_DATE
        LEFT JOIN CLB961920.ATENA_HIST   AS AH ON AH.ZCGACCOUN = SC.ZCGACCOUN AND AH.DATA_PEDIDO = CURRENT_DATE
        LEFT JOIN CLP123432.PLANILHAO_CLIENTE PC ON PC.ZCGACCOUN = SC.ZCGACCOUN    
             JOIN CLB142840.DAG40 AS DAG1 ON PC.ZCGCDMUNI||PC.ZCGCODLOC||PC.ZCGCDBAIR = DAG1.ZCGCDMUNI||DAG1.ZCGCODLOC||DAG1.ZCGCDBAIR    
       --AND DAG1.FLAG_GAV = 'S' -- desabilita aqui para liberar seleção fora do mapa    
       WHERE
           ACAO = 'DISJ' AND
           DATAREF = CURRENT_DATE AND -- suscetíveis de hoje
           FLAG_SEL = FALSE AND -- ainda não selecionados
           CH.ZCGACCOUN IS NULL AND -- não selecionado (para prevenir update Flag_sel desatualizado)
           AH.ZCGACCOUN IS NULL  -- não selecionado no Atena (ainda não gravado)
    );
        
    ELSEIF CARTEIRA = 'RECORTE' THEN
    CREATE LOCAL TEMPORARY TABLE CLB961920."#TAB" AS (
        SELECT  SC.*
        FROM CLB961920.SUSCETIVEIS AS SC
        LEFT JOIN CLB142840.CLUSTER_HIST AS CH ON CH.ZCGACCOUN = SC.ZCGACCOUN AND CH.DATA_PEDIDO = CURRENT_DATE
        LEFT JOIN CLB961920.ATENA_HIST AS AH ON AH.ZCGACCOUN = SC.ZCGACCOUN AND AH.DATA_PEDIDO = CURRENT_DATE
        WHERE
            ACAO = 'RECORTE' AND
            DATAREF = CURRENT_DATE AND -- suscetíveis de hoje
            FLAG_SEL = FALSE AND -- ainda não selecionados
            CH.ZCGACCOUN IS NULL AND -- não selecionado (para prevenir update Flag_sel desatualizado)
            AH.ZCGACCOUN IS NULL  -- não selecionado no Atena (ainda não gravado)
    );
    
    ELSEIF CARTEIRA = 'BAIXA MIX' THEN 
    
    CREATE LOCAL TEMPORARY TABLE CLB961920."#TAB" AS (
        SELECT  SC.*
        FROM CLB961920.SUSCETIVEIS AS SC
        LEFT JOIN CLB142840.CLUSTER_HIST AS CH ON CH.ZCGACCOUN = SC.ZCGACCOUN AND CH.DATA_PEDIDO = CURRENT_DATE
        LEFT JOIN CLB961920.ATENA_HIST AS AH ON AH.ZCGACCOUN = SC.ZCGACCOUN AND AH.DATA_PEDIDO = CURRENT_DATE

        WHERE
            ACAO = 'BAIXA' AND
            DATAREF = CURRENT_DATE AND -- suscetíveis de hoje
            FLAG_SEL = FALSE AND -- ainda não selecionados
            CH.ZCGACCOUN IS NULL AND -- não selecionado (para prevenir update Flag_sel desatualizado)
            AH.ZCGACCOUN IS NULL  -- não selecionado no Atena (ainda não gravado)
    );
    
    
    ELSEIF CARTEIRA = 'MISTA' AND TURMA = 'STC' THEN
    
    CREATE LOCAL TEMPORARY TABLE CLB961920."#TAB" AS (
        SELECT  CASE WHEN ACAO IN ('CORTE','DISJ') THEN 'CORTE'
                            ELSE ACAO END AS SERVICO,
        SC.*
        
        FROM CLB961920.SUSCETIVEIS AS SC
        LEFT JOIN CLB142840.CLUSTER_HIST AS CH ON CH.ZCGACCOUN = SC.ZCGACCOUN AND CH.DATA_PEDIDO = CURRENT_DATE
        LEFT JOIN CLB961920.ATENA_HIST AS AH ON AH.ZCGACCOUN = SC.ZCGACCOUN AND AH.DATA_PEDIDO = CURRENT_DATE

        WHERE
            ACAO IN ('CORTE','DISJ','RECORTE') AND 
            SC.PECLD_CONS > 40 AND 
            SC.ZCGMTVCOB > 100 AND
            DATAREF = CURRENT_DATE AND -- suscetíveis de hoje
            FLAG_SEL = FALSE AND -- ainda não selecionados
            CH.ZCGACCOUN IS NULL AND -- não selecionado (para prevenir update Flag_sel desatualizado)
            AH.ZCGACCOUN IS NULL -- não selecionado no Atena (ainda não gravado)
            
    );
    
    ELSEIF CARTEIRA = 'MISTA' AND TURMA = 'EPS' THEN
    
    CREATE LOCAL TEMPORARY TABLE CLB961920."#TAB" AS (
        SELECT  CASE WHEN ACAO IN ('CORTE','DISJ') THEN 'CORTE'
                            ELSE ACAO END AS SERVICO,
        SC.*
        
        FROM CLB961920.SUSCETIVEIS AS SC
        LEFT JOIN CLB142840.CLUSTER_HIST AS CH ON CH.ZCGACCOUN = SC.ZCGACCOUN AND CH.DATA_PEDIDO = CURRENT_DATE
        LEFT JOIN CLB961920.ATENA_HIST AS AH ON AH.ZCGACCOUN = SC.ZCGACCOUN AND AH.DATA_PEDIDO = CURRENT_DATE

        WHERE
            ACAO IN ('CORTE','DISJ','RECORTE') AND
          (ACAO NOT IN ('CORTE','DISJ') OR (
           (SC.PECLD_CONS > 20 AND ACAO IN ('CORTE','DISJ')) AND
            (SC.ZCGMTVCOB > 80 AND ACAO IN ('CORTE','DISJ'))
            )) AND
            DATAREF = CURRENT_DATE AND -- suscetíveis de hoje
            FLAG_SEL = FALSE AND -- ainda não selecionados
            CH.ZCGACCOUN IS NULL AND -- não selecionado (para prevenir update Flag_sel desatualizado)
            AH.ZCGACCOUN IS NULL 
            );-- não selecionado no Atena (ainda não gravado)
            
        
    END IF;
    
    -- DECRETO EMERGÊNCIA - CHUVAS -- (Suspender ações até meados de janeiro)
    DELETE FROM CLB961920."#TAB" 
    WHERE CURRENT_DATE < '20220222' AND (
    ZCGMUNICI IN ( -- NÃO GERAR PARA ESTES MUNICÍPIOS
'ALMADINA',
'COARACI',
'DARIO MEIRA',
'ITAJUIPE',      
'ITAPITANGA',
'UNA',
'URUCUCA'    ) 
-- NÃO GERAR PARA ZONA RURAL DESTES MUNICIPIOS
OR ((ZCGTIPLOC = 'RUR' OR ZCGBAIRRO LIKE 'RURAL%') 
AND (ZCGMUNICI IN ('JUCURUCU','ITAMARAJU','COTEGIPE','WANDERLEY')
    ))

-- NÃO GERAR PARA ESTAS LOCALIDADES (unívocas) 
OR ZCGLOCALI IN ('SAMBAITUBA', 'LAGOA ENCANTADA','TABOQUINHAS','NOVA ALEGRIA')
);
    
   --- Verificação de FERIADOS ---
    IF CARTEIRA NOT IN ('RECORTE','BAIXA MIX') THEN
        DELETE FROM CLB961920."#TAB"
        WHERE ZCGMUNICI IN 
        (SELECT DISTINCT B.MUNICIPIO FROM CLB142840.DAG40 A 
         JOIN CLB142840.DAG_CRIT C ON A.UTD40 = C.UTD
         JOIN CLB142840.ZCT_FERIADOS B ON A.ZCGCDMUNI = B.ZCGCDMUNI
         WHERE  ((C.CORTE_DIA = 'SIM' AND CARTEIRA <> 'PROPRIA') AND
             ((WEEKDAY(DATA_PEDIDO_A) <= 3 AND B.Z_DTA_FER BETWEEN ADD_DAYS(DATA_PEDIDO_A,1) AND ADD_DAYS(DATA_PEDIDO_A,2)) 
                OR (WEEKDAY(DATA_PEDIDO_A) = 4 AND B.Z_DTA_FER BETWEEN ADD_DAYS(DATA_PEDIDO_A,3) AND ADD_DAYS(DATA_PEDIDO_A,4))))
         OR 
            ((C.CORTE_DIA = 'NÃO' OR CARTEIRA ='PROPRIA') AND 
             ((WEEKDAY(DATA_PEDIDO_A) <= 2 AND B.Z_DTA_FER BETWEEN ADD_DAYS(DATA_PEDIDO_A,2) AND ADD_DAYS(DATA_PEDIDO_A,3))
                OR (WEEKDAY(DATA_PEDIDO_A) IN (3,4) AND B.Z_DTA_FER BETWEEN ADD_DAYS(DATA_PEDIDO_A,4) AND ADD_DAYS(DATA_PEDIDO_A,5))))
        AND ACAO IN ('CORTE','DISJ'));    
    END IF;
    
    
   --- ZONAS PERICULOSAS / INACESSIVEIS ---
        DELETE FROM CLB961920."#TAB"
        WHERE ZCGBAIRRO IN ('PUMBA','NORDESTE DE AMARALINA')
            OR ZCGLOCALI IN ('QUILOMBO DAS SALAMINAS','ILHA DOS FRADES','ILHA DAS FONTES','ILHA DO PATY','ILHA DE MARIA GUARDA','BOM JESUS DOS PASSOS')
            OR (UTD = 'IPIAU' AND ZCGBAIRRO IN ('RURAL-SAPINHO','RURAL-TANQUE'))
            OR (ZCGMUNICI = 'SALVADOR' AND ZCGBAIRRO = 'SANTA CRUZ')
            OR ZCGACCOUN IN (SELECT ZCGACCOUN FROM CLB961920.SUSCETIVEIS_CORTECONS WHERE ZCGBAIRRO='PERNAMBUES' AND UPPER(ZCGNMLOGR) LIKE('%TV %') AND DATAREF=CURRENT_DATE) -- Travessas de Pernambués
         ;
         
    -- Solicitação 17/02 EPS Operação
    /*DELETE FROM CLB961920."#TAB"
    WHERE ZONA IN ('0252','0348','0352')
    OR ZCGBAIRRO IN ('COSME DE FARIAS','VALE DAS PEDRINHAS');*/
    
    ------------------------------------------------------------------------------
    -- Temporário para cruzar com sqls externas (municipios em emergência) --
    IF CLB961920.TABLE_EXISTS('TEMP_ATENA','CLB961920') = 1 THEN
           DROP TABLE CLB961920.TEMP_ATENA;
    END IF;
    CREATE COLUMN TABLE CLB961920.TEMP_ATENA AS (SELECT * FROM CLB961920."#TAB");
    ------------------------------------------------------------------------------
    
    
    SELECT Min(PECLD_CONS) INTO minPECLD FROM CLB961920."#TAB";
    SELECT Max(PECLD_CONS) INTO maxPECLD FROM CLB961920."#TAB";
    
    SELECT Min(SCORE) INTO minSCORE FROM CLB961920."#TAB";
    SELECT Max(SCORE) INTO maxSCORE FROM CLB961920."#TAB";

    SELECT Min(ZCGMTVCOB) INTO minMTVCOB FROM CLB961920."#TAB";
    SELECT Max(ZCGMTVCOB) INTO maxMTVCOB FROM CLB961920."#TAB";    

    SELECT tb.*,
        34 * ((tb.PECLD_CONS-minPECLD)/(maxPECLD-minPECLD))+
        43 * ((tb.SCORE-minSCORE)/(maxSCORE-minSCORE)) +
         9 * ((tb.ZCGMTVCOB-minMTVCOB)/(maxMTVCOB-minMTVCOB))+
        -- OPERAÇÃO VERÃO
        CASE WHEN ZCGDSCNAE IN ('Hotéis','Outros alojam não especific anterior (usados como hotéis)','Apart-hotéis','Restaurantes e similares','Bares e outros estabelecimentos especializ servir bebidas','Pensões (alojamento)','Fabricação de gelo comum','Comércio atacadista de sorvetes','Fabricação de sorvetes e outros gelados comestíveis') THEN 14 ELSE 0 END AS IRR
        FROM CLB961920."#TAB" AS tb
        LEFT JOIN CLP123432.PLANILHAO_CLIENTE PC ON PC.ZCGACCOUN = tb.ZCGACCOUN; 
   
END;