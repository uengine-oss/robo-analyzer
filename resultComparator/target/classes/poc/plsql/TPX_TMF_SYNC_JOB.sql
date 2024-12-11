CREATE OR REPLACE PACKAGE TPX_TMF_SYNC_JOB IS

ROW TPJ_TMF_SYNC_JOB%ROWTYPE;
FLAG VARCHAR2(1) := 'N';

PROCEDURE SET_KEY (
    iTmfSyncJobKey    IN TPJ_TMF_SYNC_JOB.TMF_SYNC_JOB_KEY%TYPE
);

PROCEDURE INPUT (
    iRow        IN TPJ_TMF_SYNC_JOB%ROWTYPE
);

PROCEDURE INS_ROW (
    iTmfSyncJobKey  	    TPJ_TMF_SYNC_JOB.TMF_SYNC_JOB_KEY	    %TYPE,
    iTmfSyncMvrType         TPJ_TMF_SYNC_JOB.TMF_SYNC_MVR_TYPE	    %TYPE,
    iStatus                 TPJ_TMF_SYNC_JOB.STATUS 	            %TYPE,
    iProjKey     		    TPJ_TMF_SYNC_JOB.PROJ_KEY	            %TYPE,
    iUserKey     		    TPJ_TMF_SYNC_JOB.USER_KEY	            %TYPE,
    iSiteKey     		    TPJ_TMF_SYNC_JOB.SITE_KEY	            %TYPE,
    oTmfSyncJobKey    OUT   TPJ_TMF_SYNC_JOB.TMF_SYNC_JOB_KEY	    %TYPE
);

PROCEDURE UPD_STATUS (
    iTmfSyncJobKey  	    TPJ_TMF_SYNC_JOB.TMF_SYNC_JOB_KEY	    %TYPE,
    iStatus                 TPJ_TMF_SYNC_JOB.STATUS	                %TYPE,
    iProjKey     		    TPJ_TMF_SYNC_JOB.PROJ_KEY	            %TYPE,
    iUserKey     		    TPJ_TMF_SYNC_JOB.USER_KEY	            %TYPE,
    iSiteKey     		    TPJ_TMF_SYNC_JOB.SITE_KEY	            %TYPE
);


END TPX_TMF_SYNC_JOB;
/





CREATE OR REPLACE PACKAGE BODY TPX_TMF_SYNC_JOB IS


FUNCTION p_EQUAL (
    iRow    IN TPJ_TMF_SYNC_JOB%ROWTYPE
)
RETURN BOOLEAN IS
    vBool   BOOLEAN;
BEGIN
    vBool:= ROW.TMF_SYNC_JOB_KEY      	= iRow.TMF_SYNC_JOB_KEY
        AND ROW.PROJ_KEY       	        = iRow.PROJ_KEY   
        AND ROW.DEL_FLAG        	    = iRow.DEL_FLAG   
        AND ROW.SITE_KEY       	        = iRow.SITE_KEY;
    
    RETURN vBool;
END p_EQUAL;

FUNCTION p_VALUE (
    iRow    IN TPJ_TMF_SYNC_JOB%ROWTYPE
)
RETURN VARCHAR2 IS
BEGIN
    RETURN
        'TMF_SYNC_JOB_KEY : ['||ROW.TMF_SYNC_JOB_KEY    ||'] => ['||iRow.TMF_SYNC_JOB_KEY   ||']'||CHR(10)||
        'PROJ_KEY         : ['||ROW.PROJ_KEY        	||'] => ['||iRow.PROJ_KEY           ||']'||CHR(10)||
        'DEL_FLAG         : ['||ROW.DEL_FLAG            ||'] => ['||iRow.DEL_FLAG           ||']'||CHR(10)||
        'INPUT_TIME       : ['||ROW.INPUT_TIME          ||'] => ['||iRow.INPUT_TIME         ||']'||CHR(10)||
        'USER_KEY         : ['||ROW.USER_KEY            ||'] => ['||iRow.USER_KEY           ||']'||CHR(10)||
        'SITE_KEY         : ['||ROW.SITE_KEY        	||'] => ['||iRow.SITE_KEY           ||']'||CHR(10)||
        '';
END p_VALUE;

PROCEDURE p_INSERT (
    iRow    IN TPJ_TMF_SYNC_JOB%ROWTYPE
) IS
BEGIN
    BEGIN
        INSERT INTO TPJ_TMF_SYNC_JOB VALUES iRow;
    EXCEPTION WHEN OTHERS THEN
        RAISE_APPLICATION_ERROR(-20102, SQLERRM||CHR(10)||
            'CRS_ERROR: Cannot INSERT into the Table[TPJ_TMF_SYNC_JOB]'||CHR(10)||p_VALUE(iRow)||
            $$PLSQL_UNIT||'.'||$$PLSQL_LINE);
    END;
END p_INSERT;

PROCEDURE p_UPDATE (
    iRow    IN TPJ_TMF_SYNC_JOB%ROWTYPE
) IS
BEGIN
    BEGIN
        UPDATE TPJ_TMF_SYNC_JOB 
        SET ROW = iRow
        WHERE TMF_SYNC_JOB_KEY = iRow.TMF_SYNC_JOB_KEY;
    EXCEPTION WHEN OTHERS THEN
        RAISE_APPLICATION_ERROR(-20102, SQLERRM||CHR(10)||
            'CRS_ERROR: Cannot UPDATE into the Table[TPJ_TMF_SYNC_JOB]'||CHR(10)||p_VALUE(iRow)||
            $$PLSQL_UNIT||'.'||$$PLSQL_LINE);
    END;
END p_UPDATE;

FUNCTION p_GET_ROW (
    iTmfSyncJobKey      IN TPJ_TMF_SYNC_JOB.TMF_SYNC_JOB_KEY      %TYPE
)
RETURN TPJ_TMF_SYNC_JOB%ROWTYPE IS
    vRow TPJ_TMF_SYNC_JOB%ROWTYPE;
BEGIN
    SELECT * INTO vRow 
    FROM TPJ_TMF_SYNC_JOB
    WHERE TMF_SYNC_JOB_KEY     = iTmfSyncJobKey;
    
    RETURN vRow;
END p_GET_ROW;

PROCEDURE SET_KEY (
    iTmfSyncJobKey      IN TPJ_TMF_SYNC_JOB.TMF_SYNC_JOB_KEY%TYPE
) IS
BEGIN
    ROW := p_GET_ROW(iTmfSyncJobKey);
END SET_KEY;

PROCEDURE INPUT (
    iRow        IN TPJ_TMF_SYNC_JOB%ROWTYPE
) IS
    vRow        TPJ_TMF_SYNC_JOB%ROWTYPE;
BEGIN
    vRow := iRow;
    
    FLAG := 'U';
    
    BEGIN
        SET_KEY(vRow.TMF_SYNC_JOB_KEY);
    EXCEPTION WHEN NO_DATA_FOUND THEN
        FLAG := 'I';            
    END;
        
    IF FLAG = 'I' THEN
        p_INSERT(vRow);
    ELSIF p_EQUAL(vRow) = FALSE THEN
        p_UPDATE (vRow);
    ELSE FLAG := 'N'; 
    END IF;
END INPUT;

PROCEDURE INS_ROW (
    iTmfSyncJobKey  	    TPJ_TMF_SYNC_JOB.TMF_SYNC_JOB_KEY	    %TYPE
,   iTmfSyncMvrType         TPJ_TMF_SYNC_JOB.TMF_SYNC_MVR_TYPE	    %TYPE
,   iStatus                 TPJ_TMF_SYNC_JOB.STATUS	                %TYPE
,   iProjKey     		    TPJ_TMF_SYNC_JOB.PROJ_KEY	            %TYPE
,   iUserKey     		    TPJ_TMF_SYNC_JOB.USER_KEY	            %TYPE
,   iSiteKey     		    TPJ_TMF_SYNC_JOB.SITE_KEY	            %TYPE
,   oTmfSyncJobKey    OUT   TPJ_TMF_SYNC_JOB.TMF_SYNC_JOB_KEY	    %TYPE
) IS
    vRow   TPJ_TMF_SYNC_JOB%ROWTYPE;
BEGIN

    vRow.TMF_SYNC_JOB_KEY := iTmfSyncJobKey;

    vRow.STATUS         	:= iStatus;
    vRow.TMF_SYNC_MVR_TYPE  := iTmfSyncMvrType;
    vRow.PROJ_KEY        	:= iProjKey;
    vRow.DEL_FLAG           := 'N';
    vRow.INPUT_TIME         := SYSDATE;
    vRow.USER_KEY           := iUserKey;
    vRow.SITE_KEY        	:= iSiteKey;
    
    INPUT(vRow);
    
    oTmfSyncJobKey := vRow.TMF_SYNC_JOB_KEY;
    
END INS_ROW;


PROCEDURE UPD_STATUS (
    iTmfSyncJobKey  	    TPJ_TMF_SYNC_JOB.TMF_SYNC_JOB_KEY	    %TYPE
,   iStatus                 TPJ_TMF_SYNC_JOB.STATUS 	            %TYPE
,   iProjKey     		    TPJ_TMF_SYNC_JOB.PROJ_KEY	            %TYPE
,   iUserKey     		    TPJ_TMF_SYNC_JOB.USER_KEY	            %TYPE
,   iSiteKey     		    TPJ_TMF_SYNC_JOB.SITE_KEY	            %TYPE
) IS
    vRow   TPJ_TMF_SYNC_JOB%ROWTYPE;
BEGIN

    SET_KEY(iTmfSyncJobKey);

    vRow.TMF_SYNC_JOB_KEY   := iTmfSyncJobKey;
    vRow.STATUS         	:= iStatus;
    vRow.TMF_SYNC_MVR_TYPE  := ROW.TMF_SYNC_MVR_TYPE;
    vRow.PROJ_KEY        	:= iProjKey;
    vRow.DEL_FLAG           := 'N';
    vRow.INPUT_TIME         := SYSDATE;
    vRow.USER_KEY           := iUserKey;
    vRow.SITE_KEY        	:= iSiteKey;
    
    p_UPDATE(vRow);
    
END UPD_STATUS;


END TPX_TMF_SYNC_JOB;
/


