CREATE OR REPLACE PACKAGE TPX_ALARM_RECIPIENT AS

ROW TPJ_ALARM_RECIPIENT%ROWTYPE;

FLAG VARCHAR2(1) := 'N';

PROCEDURE SET_KEY (
     iAlarmKey     IN TPJ_ALARM_RECIPIENT.PROJ_ALARM_KEY%TYPE,
     iAddress      IN TPJ_ALARM_RECIPIENT.ADDRESS       %TYPE
);

PROCEDURE INPUT (
    iRow        IN TPJ_ALARM_RECIPIENT%ROWTYPE
);

PROCEDURE INS_ROW (
    iAlarmKey   		  TPJ_ALARM_RECIPIENT.PROJ_ALARM_KEY%TYPE,
    iAddress              TPJ_ALARM_RECIPIENT.ADDRESS       %TYPE,
    iRecpType             TPJ_ALARM_RECIPIENT.RECP_TYPE     %TYPE,
    iUserKey              TPJ_ALARM_RECIPIENT.USER_KEY      %TYPE,
    iUserKeyUpd           TPJ_ALARM_RECIPIENT.USER_KEY_UPD  %TYPE,
    iProjKey              TPJ_ALARM_RECIPIENT.PROJ_KEY      %TYPE,
    iSiteKey      		  TPJ_ALARM_RECIPIENT.SITE_KEY     	%TYPE
);

END TPX_ALARM_RECIPIENT;
/



CREATE OR REPLACE PACKAGE BODY TPX_ALARM_RECIPIENT AS

FUNCTION p_EQUAL (
    iRow    IN TPJ_ALARM_RECIPIENT%ROWTYPE
)
RETURN BOOLEAN IS
    vBool   BOOLEAN;
BEGIN
    vBool:= ROW.PROJ_ALARM_KEY       = iRow.PROJ_ALARM_KEY      
        AND ROW.ADDRESS              = iRow.ADDRESS             
        AND ROW.RECP_TYPE            = iRow.RECP_TYPE           
        AND NVL(TO_CHAR( ROW.USER_KEY), COM_TYPE.NULL_STR) = NVL(TO_CHAR(iRow.USER_KEY), COM_TYPE.NULL_STR)
        AND ROW.PROJ_KEY             = iRow.PROJ_KEY
        AND ROW.SITE_KEY             = iRow.SITE_KEY;
    
    RETURN vBool;
END p_EQUAL;

FUNCTION p_VALUE (
    iRow    IN TPJ_ALARM_RECIPIENT%ROWTYPE
)
RETURN VARCHAR2 IS
BEGIN
    RETURN
        'PROJ_ALARM_KEY       : ['||ROW.PROJ_ALARM_KEY      ||'] => ['||iRow.PROJ_ALARM_KEY      ||']'||CHR(10)||
        'ADDRESS              : ['||ROW.ADDRESS             ||'] => ['||iRow.ADDRESS             ||']'||CHR(10)||
        'RECP_TYPE            : ['||ROW.RECP_TYPE           ||'] => ['||iRow.RECP_TYPE           ||']'||CHR(10)||
        'USER_KEY             : ['||ROW.USER_KEY            ||'] => ['||iRow.USER_KEY            ||']'||CHR(10)||
        'INPUT_TIME           : ['||ROW.INPUT_TIME          ||'] => ['||iRow.INPUT_TIME          ||']'||CHR(10)||
        'USER_KEY_UPD         : ['||ROW.USER_KEY_UPD        ||'] => ['||iRow.USER_KEY_UPD        ||']'||CHR(10)||
        'PROJ_KEY             : ['||ROW.PROJ_KEY            ||'] => ['||iRow.PROJ_KEY            ||']'||CHR(10)||
        'SITE_KEY             : ['||ROW.SITE_KEY            ||'] => ['||iRow.SITE_KEY            ||']'||CHR(10)||
        '';
END p_VALUE;

PROCEDURE p_INSERT (
    iRow    IN TPJ_ALARM_RECIPIENT%ROWTYPE
) IS
BEGIN
    BEGIN
        INSERT INTO TPJ_ALARM_RECIPIENT VALUES iRow;
    EXCEPTION WHEN OTHERS THEN
        RAISE_APPLICATION_ERROR(-20102, SQLERRM||CHR(10)||
            'CRS_ERROR: Cannot INSERT into the Table[TPJ_ALARM_RECIPIENT]'||CHR(10)||p_VALUE(iRow)||
            $$PLSQL_UNIT||'.'||$$PLSQL_LINE);
    END;
END p_INSERT;

PROCEDURE p_UPDATE (
    iRow    IN TPJ_ALARM_RECIPIENT%ROWTYPE
) IS
BEGIN
    BEGIN
        UPDATE TPJ_ALARM_RECIPIENT SET ROW = iRow
        WHERE PROJ_ALARM_KEY = iRow.PROJ_ALARM_KEY
		  AND ADDRESS = iRow.ADDRESS;
    EXCEPTION WHEN OTHERS THEN
        RAISE_APPLICATION_ERROR(-20102, SQLERRM||CHR(10)||
            'CRS_ERROR: Cannot UPDATE into the Table[TPJ_ALARM_RECIPIENT]'||CHR(10)||p_VALUE(iRow)||
            $$PLSQL_UNIT||'.'||$$PLSQL_LINE);
    END;
END p_UPDATE;

FUNCTION p_GET_ROW (
     iAlarmKey     IN TPJ_ALARM_RECIPIENT.PROJ_ALARM_KEY%TYPE,
     iAddress      IN TPJ_ALARM_RECIPIENT.ADDRESS       %TYPE
)
RETURN TPJ_ALARM_RECIPIENT%ROWTYPE IS
    vRow TPJ_ALARM_RECIPIENT%ROWTYPE;
BEGIN
    SELECT * INTO vRow FROM TPJ_ALARM_RECIPIENT
    WHERE PROJ_ALARM_KEY = iAlarmKey
	  AND ADDRESS = iAddress;
    
    RETURN vRow;
END p_GET_ROW;

PROCEDURE SET_KEY (
     iAlarmKey     IN TPJ_ALARM_RECIPIENT.PROJ_ALARM_KEY%TYPE
,    iAddress      IN TPJ_ALARM_RECIPIENT.ADDRESS       %TYPE
) IS
BEGIN
    ROW := p_GET_ROW(iAlarmKey, iAddress);
END SET_KEY;

PROCEDURE INPUT (
    iRow        IN TPJ_ALARM_RECIPIENT%ROWTYPE
) IS
    vRow        TPJ_ALARM_RECIPIENT   %ROWTYPE;
BEGIN
    vRow := iRow;
    
    FLAG := 'U';
    
    BEGIN
        SET_KEY(vRow.PROJ_ALARM_KEY, vRow.ADDRESS);
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
    iAlarmKey   		  TPJ_ALARM_RECIPIENT.PROJ_ALARM_KEY%TYPE,
    iAddress              TPJ_ALARM_RECIPIENT.ADDRESS       %TYPE,
    iRecpType             TPJ_ALARM_RECIPIENT.RECP_TYPE     %TYPE,
    iUserKey              TPJ_ALARM_RECIPIENT.USER_KEY      %TYPE,
    iUserKeyUpd           TPJ_ALARM_RECIPIENT.USER_KEY_UPD  %TYPE,
    iProjKey              TPJ_ALARM_RECIPIENT.PROJ_KEY      %TYPE,
    iSiteKey      		  TPJ_ALARM_RECIPIENT.SITE_KEY     	%TYPE
) IS
    vRow   TPJ_ALARM_RECIPIENT%ROWTYPE;
BEGIN
    vRow.PROJ_ALARM_KEY       := iAlarmKey;
    vRow.ADDRESS              := iAddress;
    vRow.RECP_TYPE            := iRecpType;
    vRow.USER_KEY             := iUserKey;
    vRow.INPUT_TIME           := SYSDATE;
    vRow.USER_KEY_UPD         := iUserKeyUpd;
    vRow.PROJ_KEY             := iProjKey;
    vRow.SITE_KEY       	  := iSiteKey;
    
    INPUT(vRow);
END INS_ROW;

END TPX_ALARM_RECIPIENT;
/

