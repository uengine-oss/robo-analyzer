CREATE OR REPLACE PACKAGE TPX_ALARM_CONTENT AS

ROW TPJ_ALARM_CONTENT%ROWTYPE;

FLAG VARCHAR2(1) := 'N';

PROCEDURE SET_KEY (
     iAlarmKey     IN TPJ_ALARM_CONTENT.PROJ_ALARM_KEY%TYPE
);

PROCEDURE INPUT (
    iRow        IN TPJ_ALARM_CONTENT%ROWTYPE
);

PROCEDURE INS_ROW (
    iAlarmKey   	IN 	TPJ_ALARM_CONTENT.PROJ_ALARM_KEY%TYPE,
    iContent  		IN 	TPJ_ALARM_CONTENT.CONTENT  	  	%TYPE,
    iUserKey      	IN 	TPJ_ALARM_CONTENT.USER_KEY     	%TYPE,
    iProjKey      	IN 	TPJ_ALARM_CONTENT.PROJ_KEY     	%TYPE,
    iSiteKey      	IN 	TPJ_ALARM_CONTENT.SITE_KEY     	%TYPE
);

END TPX_ALARM_CONTENT;
/





CREATE OR REPLACE PACKAGE BODY TPX_ALARM_CONTENT AS

FUNCTION p_EQUAL (
    iRow    IN TPJ_ALARM_CONTENT%ROWTYPE
)
RETURN BOOLEAN IS
    vBool   BOOLEAN;
BEGIN
    vBool:= ROW.PROJ_ALARM_KEY       = iRow.PROJ_ALARM_KEY      
        AND ROW.CONTENT              = iRow.CONTENT             
        AND ROW.PROJ_KEY             = iRow.PROJ_KEY
        AND ROW.SITE_KEY             = iRow.SITE_KEY;
    
    RETURN vBool;
END p_EQUAL;

FUNCTION p_VALUE (
    iRow    IN TPJ_ALARM_CONTENT%ROWTYPE
)
RETURN VARCHAR2 IS
BEGIN
    RETURN
        'PROJ_ALARM_KEY       : ['||ROW.PROJ_ALARM_KEY      ||'] => ['||iRow.PROJ_ALARM_KEY      ||']'||CHR(10)||
        'CONTENT              : ['||ROW.CONTENT             ||'] => ['||iRow.CONTENT             ||']'||CHR(10)||
        'INPUT_TIME           : ['||ROW.INPUT_TIME          ||'] => ['||iRow.INPUT_TIME          ||']'||CHR(10)||
        'USER_KEY             : ['||ROW.USER_KEY            ||'] => ['||iRow.USER_KEY            ||']'||CHR(10)||
        'PROJ_KEY             : ['||ROW.PROJ_KEY            ||'] => ['||iRow.PROJ_KEY            ||']'||CHR(10)||
        'SITE_KEY             : ['||ROW.SITE_KEY            ||'] => ['||iRow.SITE_KEY            ||']'||CHR(10)||
        '';
END p_VALUE;

PROCEDURE p_INSERT (
    iRow    IN TPJ_ALARM_CONTENT%ROWTYPE
) IS
BEGIN
    BEGIN
        INSERT INTO TPJ_ALARM_CONTENT VALUES iRow;
    EXCEPTION WHEN OTHERS THEN
        RAISE_APPLICATION_ERROR(-20102, SQLERRM||CHR(10)||
            'CRS_ERROR: Cannot INSERT into the Table[TPJ_ALARM_CONTENT]'||CHR(10)||p_VALUE(iRow)||
            $$PLSQL_UNIT||'.'||$$PLSQL_LINE);
    END;
END p_INSERT;

PROCEDURE p_UPDATE (
    iRow    IN TPJ_ALARM_CONTENT%ROWTYPE
) IS
BEGIN
    BEGIN
        UPDATE TPJ_ALARM_CONTENT SET ROW = iRow
        WHERE PROJ_ALARM_KEY = iRow.PROJ_ALARM_KEY;
    EXCEPTION WHEN OTHERS THEN
        RAISE_APPLICATION_ERROR(-20102, SQLERRM||CHR(10)||
            'CRS_ERROR: Cannot UPDATE into the Table[TPJ_ALARM_CONTENT]'||CHR(10)||p_VALUE(iRow)||
            $$PLSQL_UNIT||'.'||$$PLSQL_LINE);
    END;
END p_UPDATE;

FUNCTION p_GET_ROW (
     iAlarmKey     IN TPJ_ALARM_CONTENT.PROJ_ALARM_KEY%TYPE
)
RETURN TPJ_ALARM_CONTENT%ROWTYPE IS
    vRow TPJ_ALARM_CONTENT%ROWTYPE;
BEGIN
    SELECT * INTO vRow FROM TPJ_ALARM_CONTENT
    WHERE PROJ_ALARM_KEY = iAlarmKey;
    
    RETURN vRow;
END p_GET_ROW;

PROCEDURE SET_KEY (
     iAlarmKey     IN TPJ_ALARM_CONTENT.PROJ_ALARM_KEY%TYPE
) IS
BEGIN
    ROW := p_GET_ROW(iAlarmKey);
END SET_KEY;

PROCEDURE INPUT (
    iRow        IN TPJ_ALARM_CONTENT%ROWTYPE
) IS
    vRow        TPJ_ALARM_CONTENT   %ROWTYPE;
BEGIN
    vRow := iRow;
    
    FLAG := 'U';
    
    BEGIN
        SET_KEY(vRow.PROJ_ALARM_KEY);
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
    iAlarmKey   	IN 	TPJ_ALARM_CONTENT.PROJ_ALARM_KEY%TYPE,
    iContent  		IN 	TPJ_ALARM_CONTENT.CONTENT  	  	%TYPE,
    iUserKey      	IN 	TPJ_ALARM_CONTENT.USER_KEY     	%TYPE,
    iProjKey      	IN 	TPJ_ALARM_CONTENT.PROJ_KEY     	%TYPE,
    iSiteKey      	IN 	TPJ_ALARM_CONTENT.SITE_KEY     	%TYPE
) IS
    vRow   TPJ_ALARM_CONTENT%ROWTYPE;
BEGIN
    vRow.PROJ_ALARM_KEY	:= iAlarmKey;
    vRow.CONTENT     	:= iContent;
    vRow.INPUT_TIME     := SYSDATE;
    vRow.USER_KEY       := iUserKey;
    vRow.PROJ_KEY       := iProjKey;
    vRow.SITE_KEY       := iSiteKey;
    
    INPUT(vRow);
END INS_ROW;

END TPX_ALARM_CONTENT;
/