ALTER SESSION SET PLSQL_CODE_TYPE = 'INTERPRETED';

CREATE OR REPLACE PACKAGE TPX_TMF_SYNC_JOB_STATUS AS

    ROW TPJ_TMF_SYNC_JOB_STATUS%ROWTYPE;
    FLAG VARCHAR2(1) := 'N';

    PROCEDURE SET_KEY (
        iTmfSyncJobKey      IN TPJ_TMF_SYNC_JOB_STATUS.TMF_SYNC_JOB_KEY %TYPE,
        iTscKey      	    IN TPJ_TMF_SYNC_JOB_STATUS.TSC_KEY	        %TYPE
    );

    PROCEDURE INPUT (
        iRow        IN TPJ_TMF_SYNC_JOB_STATUS%ROWTYPE
    );

    PROCEDURE INS_ROW (
        iTmfSyncJobKey  		TPJ_TMF_SYNC_JOB_STATUS.TMF_SYNC_JOB_KEY%TYPE,
        iTscKey      		    TPJ_TMF_SYNC_JOB_STATUS.SITE_KEY	    %TYPE,
        iProjKey     		    TPJ_TMF_SYNC_JOB_STATUS.PROJ_KEY	    %TYPE,
        iStatus     		    TPJ_TMF_SYNC_JOB_STATUS.STATUS	 	    %TYPE,
        iUserKey     		    TPJ_TMF_SYNC_JOB_STATUS.USER_KEY	    %TYPE,
        iSiteKey     		    TPJ_TMF_SYNC_JOB_STATUS.SITE_KEY	    %TYPE
    );

    PROCEDURE UPD_STATUS (
        iTmfSyncJobKey  		TPJ_TMF_SYNC_JOB_STATUS.TMF_SYNC_JOB_KEY%TYPE,
        iTscKey      		    TPJ_TMF_SYNC_JOB_STATUS.SITE_KEY	    %TYPE,
        iProjKey     		    TPJ_TMF_SYNC_JOB_STATUS.PROJ_KEY	    %TYPE,
        iStatus     		    TPJ_TMF_SYNC_JOB_STATUS.STATUS	 	    %TYPE,
        iUserKey     		    TPJ_TMF_SYNC_JOB_STATUS.USER_KEY	    %TYPE,
        iSiteKey     		    TPJ_TMF_SYNC_JOB_STATUS.SITE_KEY	    %TYPE
    );

    PROCEDURE UPD_BATCH_FAIL (
        iTmfSyncJobKey  		TPJ_TMF_SYNC_JOB_STATUS.TMF_SYNC_JOB_KEY%TYPE,
        iTscKey      		    TPJ_TMF_SYNC_JOB_STATUS.SITE_KEY	    %TYPE,
        iProjKey     		    TPJ_TMF_SYNC_JOB_STATUS.PROJ_KEY	    %TYPE,
        iStatus     		    TPJ_TMF_SYNC_JOB_STATUS.STATUS	 	    %TYPE,
        iUserKey     		    TPJ_TMF_SYNC_JOB_STATUS.USER_KEY	    %TYPE,
        iSiteKey     		    TPJ_TMF_SYNC_JOB_STATUS.SITE_KEY	    %TYPE
    );


    PROCEDURE UPD_TMF_JOB_KEY (
        iTmfSyncJobKey  		TPJ_TMF_SYNC_JOB_STATUS.TMF_SYNC_JOB_KEY%TYPE,
        iTscKey      		    TPJ_TMF_SYNC_JOB_STATUS.SITE_KEY	    %TYPE,
        iProjKey     		    TPJ_TMF_SYNC_JOB_STATUS.PROJ_KEY	    %TYPE,
        iTmfJobKey     		    TPJ_TMF_SYNC_JOB_STATUS.TMF_JOB_KEY	    %TYPE,
        iStatus     		    TPJ_TMF_SYNC_JOB_STATUS.STATUS	 	    %TYPE,
        iUserKey     		    TPJ_TMF_SYNC_JOB_STATUS.USER_KEY	    %TYPE,
        iSiteKey     		    TPJ_TMF_SYNC_JOB_STATUS.SITE_KEY	    %TYPE
    );

END TPX_TMF_SYNC_JOB_STATUS;
/





CREATE OR REPLACE PACKAGE BODY TPX_TMF_SYNC_JOB_STATUS AS

    FUNCTION p_EQUAL (
        iRow    IN TPJ_TMF_SYNC_JOB_STATUS%ROWTYPE
    )
    RETURN BOOLEAN IS
        vBool   BOOLEAN;
    BEGIN
        vBool:= ROW.TMF_SYNC_JOB_KEY      	= iRow.TMF_SYNC_JOB_KEY
            AND ROW.TSC_KEY       	        = iRow.TSC_KEY
            AND ROW.PROJ_KEY       	        = iRow.PROJ_KEY   
            AND ROW.START_TIME       	    = iRow.START_TIME   
            AND NVL(TO_CHAR(ROW.END_TIME, 'YYYY-MM-DD HH24:MI:SS'), COM_TYPE.NULL_STR) = 
                NVL(TO_CHAR(iRow.END_TIME, 'YYYY-MM-DD HH24:MI:SS'), COM_TYPE.NULL_STR)
            AND ROW.STATUS        		    = iRow.STATUS   
            AND ROW.DEL_FLAG        	    = iRow.DEL_FLAG   
            AND ROW.SITE_KEY       	        = iRow.SITE_KEY;
        RETURN vBool;
    END p_EQUAL;

    FUNCTION p_VALUE (
        iRow    IN TPJ_TMF_SYNC_JOB_STATUS%ROWTYPE
    )
    RETURN VARCHAR2 IS
    BEGIN
        RETURN
            'TMF_SYNC_JOB_KEY   : ['||ROW.TMF_SYNC_JOB_KEY   ||'] => ['||iRow.TMF_SYNC_JOB_KEY  ||']'||CHR(10)||
            'TSC_KEY            : ['||ROW.TSC_KEY        	 ||'] => ['||iRow.TSC_KEY           ||']'||CHR(10)||
            'PROJ_KEY           : ['||ROW.PROJ_KEY        	 ||'] => ['||iRow.PROJ_KEY          ||']'||CHR(10)||
            'START_TIME         : ['||ROW.START_TIME         ||'] => ['||iRow.START_TIME        ||']'||CHR(10)||
            'END_TIME           : ['||ROW.END_TIME			 ||'] => ['||iRow.END_TIME 			||']'||CHR(10)||
            'STATUS             : ['||ROW.STATUS         	 ||'] => ['||iRow.STATUS       		||']'||CHR(10)||
            'DEL_FLAG           : ['||ROW.DEL_FLAG           ||'] => ['||iRow.DEL_FLAG          ||']'||CHR(10)||
            'INPUT_TIME         : ['||ROW.INPUT_TIME         ||'] => ['||iRow.INPUT_TIME        ||']'||CHR(10)||
                'USER_KEY       : ['||ROW.USER_KEY           ||'] => ['||iRow.USER_KEY          ||']'||CHR(10)||
            '';
    END p_VALUE;

    PROCEDURE p_INSERT (
        iRow    IN TPJ_TMF_SYNC_JOB_STATUS%ROWTYPE
    ) IS
    BEGIN
        BEGIN
            INSERT INTO TPJ_TMF_SYNC_JOB_STATUS VALUES iRow;
        EXCEPTION WHEN OTHERS THEN
            RAISE_APPLICATION_ERROR(-20102, SQLERRM||CHR(10)||
                'CRS_ERROR: Cannot INSERT into the Table[TPJ_TMF_SYNC_JOB_STATUS]'||CHR(10)||p_VALUE(iRow)||
                $$PLSQL_UNIT||'.'||$$PLSQL_LINE);
        END;
    END p_INSERT;

    PROCEDURE p_UPDATE (
        iRow    IN TPJ_TMF_SYNC_JOB_STATUS%ROWTYPE
    ) IS
    BEGIN
        BEGIN
            UPDATE TPJ_TMF_SYNC_JOB_STATUS 
            SET ROW = iRow
            WHERE TMF_SYNC_JOB_KEY  = iRow.TMF_SYNC_JOB_KEY
            AND TSC_KEY           = iRow.TSC_KEY;
        EXCEPTION WHEN OTHERS THEN
            RAISE_APPLICATION_ERROR(-20102, SQLERRM||CHR(10)||
                'CRS_ERROR: Cannot UPDATE into the Table[TPJ_TMF_SYNC_JOB_STATUS]'||CHR(10)||p_VALUE(iRow)||
                $$PLSQL_UNIT||'.'||$$PLSQL_LINE);
        END;
    END p_UPDATE;

    FUNCTION p_GET_ROW (
        iTmfSyncJobKey      IN TPJ_TMF_SYNC_JOB_STATUS.TMF_SYNC_JOB_KEY      %TYPE,
        iTscKey     	    IN TPJ_TMF_SYNC_JOB_STATUS.TSC_KEY	             %TYPE
    )
    RETURN TPJ_TMF_SYNC_JOB_STATUS%ROWTYPE IS
        vRow TPJ_TMF_SYNC_JOB_STATUS%ROWTYPE;
    BEGIN
        SELECT * INTO vRow 
        FROM TPJ_TMF_SYNC_JOB_STATUS
        WHERE TMF_SYNC_JOB_KEY      = iTmfSyncJobKey
        AND TSC_KEY              = iTscKey;
        
        RETURN vRow;
    END p_GET_ROW;

    PROCEDURE SET_KEY (
        iTmfSyncJobKey      IN TPJ_TMF_SYNC_JOB_STATUS.TMF_SYNC_JOB_KEY%TYPE,
        iTscKey      	  IN TPJ_TMF_SYNC_JOB_STATUS.TSC_KEY	  %TYPE
    ) IS
    BEGIN
        ROW := p_GET_ROW(iTmfSyncJobKey, iTscKey);
    END SET_KEY;

    PROCEDURE INPUT (
        iRow        IN TPJ_TMF_SYNC_JOB_STATUS%ROWTYPE
    )IS
        vRow        TPJ_TMF_SYNC_JOB_STATUS%ROWTYPE;
    BEGIN
        vRow := iRow;
        
        FLAG := 'U';
        
        BEGIN
            SET_KEY(vRow.TMF_SYNC_JOB_KEY, vRow.TSC_KEY);
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
        iTmfSyncJobKey  		TPJ_TMF_SYNC_JOB_STATUS.TMF_SYNC_JOB_KEY%TYPE,
        iTscKey      		    TPJ_TMF_SYNC_JOB_STATUS.SITE_KEY	    %TYPE,
        iProjKey     		    TPJ_TMF_SYNC_JOB_STATUS.PROJ_KEY	    %TYPE,
        iStatus     		    TPJ_TMF_SYNC_JOB_STATUS.STATUS	 	    %TYPE,
        iUserKey     		    TPJ_TMF_SYNC_JOB_STATUS.USER_KEY	    %TYPE,
        iSiteKey     		    TPJ_TMF_SYNC_JOB_STATUS.SITE_KEY	    %TYPE
    ) IS
        vRow   TPJ_TMF_SYNC_JOB_STATUS%ROWTYPE;
        vCurrentTime    DATE;
    BEGIN
        BEGIN
            TPX_PROJECT.SET_KEY(iProjKey);
        EXCEPTION WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20999, 'CRS_ERROR:'||CHR(10)||'[MSG]Project Information does Not Exist.[MSG]');
        END;

        vCurrentTime            := SYSDATE;

        vRow.TMF_SYNC_JOB_KEY   := iTmfSyncJobKey;
        vRow.TSC_KEY        	:= iTscKey;
        vRow.PROJ_KEY        	:= iProjKey;
        vRow.STATUS         	:= iStatus;
        vRow.START_TIME         := vCurrentTime;
        vRow.DEL_FLAG           := 'N';
        vRow.INPUT_TIME         := vCurrentTime;
        vRow.USER_KEY           := iUserKey;
        vRow.SITE_KEY           := iSiteKey;
        
        INPUT(vRow);
        
    END INS_ROW;


    PROCEDURE UPD_STATUS (
        iTmfSyncJobKey  		TPJ_TMF_SYNC_JOB_STATUS.TMF_SYNC_JOB_KEY%TYPE,
        iTscKey      		    TPJ_TMF_SYNC_JOB_STATUS.SITE_KEY	    %TYPE,
        iProjKey     		    TPJ_TMF_SYNC_JOB_STATUS.PROJ_KEY	    %TYPE,
        iStatus     		    TPJ_TMF_SYNC_JOB_STATUS.STATUS	 	    %TYPE,
        iUserKey     		    TPJ_TMF_SYNC_JOB_STATUS.USER_KEY	    %TYPE,
        iSiteKey     		    TPJ_TMF_SYNC_JOB_STATUS.SITE_KEY	    %TYPE
    ) IS
        vRow   TPJ_TMF_SYNC_JOB_STATUS%ROWTYPE;
        vCurrentTime    DATE;
    BEGIN
        
        BEGIN
            TPX_PROJECT.SET_KEY(iProjKey);
            SET_KEY(iTmfSyncJobKey, iTscKey);
        EXCEPTION WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20999, 'CRS_ERROR:'||CHR(10)||'[MSG]Project Information does Not Exist.[MSG]');
        END;

        vCurrentTime            := SYSDATE;

        vRow.TMF_SYNC_JOB_KEY   := iTmfSyncJobKey;
        vRow.TMF_JOB_KEY        := ROW.TMF_JOB_KEY;
        vRow.TSC_KEY        	:= iTscKey;
        vRow.PROJ_KEY        	:= iProjKey;
        vRow.STATUS         	:= iStatus;
        vRow.START_TIME         := ROW.START_TIME;
        vRow.END_TIME           := vCurrentTime;
        vRow.DEL_FLAG           := 'N';
        vRow.INPUT_TIME         := vCurrentTime;
        vRow.USER_KEY           := iUserKey;
        vRow.SITE_KEY           := iSiteKey;
        
        p_UPDATE(vRow);
        
    END UPD_STATUS;


    PROCEDURE UPD_BATCH_FAIL (
        iTmfSyncJobKey  		TPJ_TMF_SYNC_JOB_STATUS.TMF_SYNC_JOB_KEY%TYPE,
        iTscKey      		    TPJ_TMF_SYNC_JOB_STATUS.SITE_KEY	    %TYPE,
        iProjKey     		    TPJ_TMF_SYNC_JOB_STATUS.PROJ_KEY	    %TYPE,
        iStatus     		    TPJ_TMF_SYNC_JOB_STATUS.STATUS	 	    %TYPE,
        iUserKey     		    TPJ_TMF_SYNC_JOB_STATUS.USER_KEY	    %TYPE,
        iSiteKey     		    TPJ_TMF_SYNC_JOB_STATUS.SITE_KEY	    %TYPE
    ) IS
        vRow   TPJ_TMF_SYNC_JOB_STATUS%ROWTYPE;
        vCurrentTime    DATE;
    BEGIN
        BEGIN
            TPX_PROJECT.SET_KEY(iProjKey);
        EXCEPTION WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20999, 'CRS_ERROR:'||CHR(10)||'[MSG]Project Information does Not Exist.[MSG]');
        END;

        vCurrentTime            := SYSDATE;

        vRow.TMF_SYNC_JOB_KEY   := iTmfSyncJobKey;
        vRow.TSC_KEY        	:= iTscKey;
        vRow.PROJ_KEY        	:= iProjKey;
        vRow.STATUS         	:= iStatus;
        vRow.START_TIME         := vCurrentTime;
        vRow.END_TIME           := vCurrentTime;
        vRow.DEL_FLAG           := 'N';
        vRow.INPUT_TIME         := vCurrentTime;
        vRow.USER_KEY           := iUserKey;
        vRow.SITE_KEY           := iSiteKey;
        
        INPUT(vRow);
        
    END UPD_BATCH_FAIL;

    PROCEDURE UPD_TMF_JOB_KEY (
        iTmfSyncJobKey  		TPJ_TMF_SYNC_JOB_STATUS.TMF_SYNC_JOB_KEY%TYPE,
        iTscKey      		    TPJ_TMF_SYNC_JOB_STATUS.SITE_KEY	    %TYPE,
        iProjKey     		    TPJ_TMF_SYNC_JOB_STATUS.PROJ_KEY	    %TYPE,
        iTmfJobKey     		    TPJ_TMF_SYNC_JOB_STATUS.TMF_JOB_KEY	    %TYPE,
        iStatus     		    TPJ_TMF_SYNC_JOB_STATUS.STATUS	 	    %TYPE,
        iUserKey     		    TPJ_TMF_SYNC_JOB_STATUS.USER_KEY	    %TYPE,
        iSiteKey     		    TPJ_TMF_SYNC_JOB_STATUS.SITE_KEY	    %TYPE
    ) IS
        vRow   TPJ_TMF_SYNC_JOB_STATUS%ROWTYPE;
        vCurrentTime    DATE;
    BEGIN
        BEGIN
            TPX_PROJECT.SET_KEY(iProjKey);
            SET_KEY(iTmfSyncJobKey, iTscKey);
        EXCEPTION WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20999, 'CRS_ERROR:'||CHR(10)||'[MSG]Project Information does Not Exist.[MSG]');
        END;

        vCurrentTime            := SYSDATE;

        vRow.TMF_SYNC_JOB_KEY   := iTmfSyncJobKey;
        vRow.TSC_KEY        	:= iTscKey;
        vRow.PROJ_KEY        	:= iProjKey;
        VRow.TMF_JOB_KEY        := iTmfJobKey;
        vRow.STATUS         	:= iStatus;
        vRow.START_TIME         := ROW.START_TIME;
        vRow.DEL_FLAG           := 'N';
        vRow.INPUT_TIME         := vCurrentTime;
        vRow.USER_KEY           := iUserKey;
        vRow.SITE_KEY           := iSiteKey;
        
        p_UPDATE(vRow);
        
    END UPD_TMF_JOB_KEY;


END TPX_TMF_SYNC_JOB_STATUS;
/