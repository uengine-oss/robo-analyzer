CREATE OR REPLACE PACKAGE TPX_ALARM_FILE AS

ROW TPJ_ALARM_FILE%ROWTYPE;

FLAG VARCHAR2(1) := 'N';

PROCEDURE SET_KEY (
    iAlarmKey      IN TPJ_ALARM_FILE.PROJ_ALARM_KEY%TYPE,
    iFilePath      IN TPJ_ALARM_FILE.FILE_PATH     %TYPE,
    iFileName      IN TPJ_ALARM_FILE.FILE_NAME     %TYPE
);

PROCEDURE INPUT (
    iRow        IN TPJ_ALARM_FILE%ROWTYPE
);

PROCEDURE INS_ROW (
    iAlarmKey   		  TPJ_ALARM_FILE.PROJ_ALARM_KEY%TYPE,
    iFilePath             TPJ_ALARM_FILE.FILE_PATH     %TYPE,
    iFileName             TPJ_ALARM_FILE.FILE_NAME     %TYPE,
    iUserKey              TPJ_ALARM_FILE.USER_KEY      %TYPE,
    iProjKey              TPJ_ALARM_FILE.PROJ_KEY      %TYPE,
    iSiteKey      		  TPJ_ALARM_FILE.SITE_KEY      %TYPE
);

END TPX_ALARM_FILE;
/





CREATE OR REPLACE PACKAGE BODY TPX_ALARM_FILE AS

FUNCTION p_EQUAL (
    iRow    IN TPJ_ALARM_FILE%ROWTYPE
)
RETURN BOOLEAN IS
    vBool   BOOLEAN;
BEGIN
    vBool:= ROW.PROJ_ALARM_KEY       = iRow.PROJ_ALARM_KEY      
        AND ROW.FILE_PATH            = iRow.FILE_PATH             
        AND ROW.FILE_NAME            = iRow.FILE_NAME           
        AND ROW.PROJ_KEY             = iRow.PROJ_KEY
        AND ROW.SITE_KEY             = iRow.SITE_KEY;
    
    RETURN vBool;
END p_EQUAL;

FUNCTION p_VALUE (
    iRow    IN TPJ_ALARM_FILE%ROWTYPE
)
RETURN VARCHAR2 IS
BEGIN
    RETURN
        'PROJ_ALARM_KEY       : ['||ROW.PROJ_ALARM_KEY      ||'] => ['||iRow.PROJ_ALARM_KEY      ||']'||CHR(10)||
        'FILE_PATH            : ['||ROW.FILE_PATH           ||'] => ['||iRow.FILE_PATH           ||']'||CHR(10)||
        'FILE_NAME            : ['||ROW.FILE_NAME           ||'] => ['||iRow.FILE_NAME           ||']'||CHR(10)||
        'INPUT_TIME           : ['||ROW.INPUT_TIME          ||'] => ['||iRow.INPUT_TIME          ||']'||CHR(10)||
        'USER_KEY             : ['||ROW.USER_KEY            ||'] => ['||iRow.USER_KEY            ||']'||CHR(10)||
        'PROJ_KEY             : ['||ROW.PROJ_KEY            ||'] => ['||iRow.PROJ_KEY            ||']'||CHR(10)||
        'SITE_KEY             : ['||ROW.SITE_KEY            ||'] => ['||iRow.SITE_KEY            ||']'||CHR(10)||
        '';
END p_VALUE;

PROCEDURE p_INSERT (
    iRow    IN TPJ_ALARM_FILE%ROWTYPE
) IS
BEGIN
    BEGIN
        INSERT INTO TPJ_ALARM_FILE VALUES iRow;
    EXCEPTION WHEN OTHERS THEN
        RAISE_APPLICATION_ERROR(-20102, SQLERRM||CHR(10)||
            'CRS_ERROR: Cannot INSERT into the Table[TPJ_ALARM_FILE]'||CHR(10)||p_VALUE(iRow)||
            $$PLSQL_UNIT||'.'||$$PLSQL_LINE);
    END;
END p_INSERT;

PROCEDURE p_UPDATE (
    iRow    IN TPJ_ALARM_FILE%ROWTYPE
) IS
BEGIN
    BEGIN
        UPDATE TPJ_ALARM_FILE SET ROW = iRow
        WHERE PROJ_ALARM_KEY = iRow.PROJ_ALARM_KEY
		  AND FILE_PATH = iRow.FILE_PATH
		  AND FILE_NAME = iRow.FILE_NAME;
    EXCEPTION WHEN OTHERS THEN
        RAISE_APPLICATION_ERROR(-20102, SQLERRM||CHR(10)||
            'CRS_ERROR: Cannot UPDATE into the Table[TPJ_ALARM_FILE]'||CHR(10)||p_VALUE(iRow)||
            $$PLSQL_UNIT||'.'||$$PLSQL_LINE);
    END;
END p_UPDATE;

FUNCTION p_GET_ROW (
    iAlarmKey      IN TPJ_ALARM_FILE.PROJ_ALARM_KEY%TYPE,
    iFilePath      IN TPJ_ALARM_FILE.FILE_PATH     %TYPE,
    iFileName      IN TPJ_ALARM_FILE.FILE_NAME     %TYPE

)
RETURN TPJ_ALARM_FILE%ROWTYPE IS
    vRow TPJ_ALARM_FILE%ROWTYPE;
BEGIN
    SELECT * INTO vRow FROM TPJ_ALARM_FILE
    WHERE PROJ_ALARM_KEY = iAlarmKey
	  AND FILE_PATH = iFilePath
	  AND FILE_NAME = iFileName;
    
    RETURN vRow;
END p_GET_ROW;

PROCEDURE SET_KEY (
    iAlarmKey      IN TPJ_ALARM_FILE.PROJ_ALARM_KEY%TYPE,
    iFilePath      IN TPJ_ALARM_FILE.FILE_PATH     %TYPE,
    iFileName      IN TPJ_ALARM_FILE.FILE_NAME     %TYPE

) IS
BEGIN
    ROW := p_GET_ROW(iAlarmKey, iFilePath, iFileName);
END SET_KEY;

PROCEDURE INPUT (
    iRow        IN TPJ_ALARM_FILE%ROWTYPE
) IS
    vRow        TPJ_ALARM_FILE   %ROWTYPE;
BEGIN
    vRow := iRow;
    
    FLAG := 'U';
    
    BEGIN
        SET_KEY(vRow.PROJ_ALARM_KEY, vRow.FILE_PATH, vRow.FILE_NAME);
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
    iAlarmKey   		  TPJ_ALARM_FILE.PROJ_ALARM_KEY%TYPE,
    iFilePath             TPJ_ALARM_FILE.FILE_PATH     %TYPE,
    iFileName             TPJ_ALARM_FILE.FILE_NAME     %TYPE,
    iUserKey              TPJ_ALARM_FILE.USER_KEY      %TYPE,
    iProjKey              TPJ_ALARM_FILE.PROJ_KEY      %TYPE,
    iSiteKey      		  TPJ_ALARM_FILE.SITE_KEY      %TYPE
) IS
    vRow   TPJ_ALARM_FILE%ROWTYPE;
BEGIN
    vRow.PROJ_ALARM_KEY       := iAlarmKey;
    vRow.FILE_PATH            := iFilePath;
    vRow.FILE_NAME            := iFileName;
    vRow.INPUT_TIME           := SYSDATE;
    vRow.USER_KEY             := iUserKey;
    vRow.PROJ_KEY             := iProjKey;
    vRow.SITE_KEY       	  := iSiteKey;
    
    INPUT(vRow);
END INS_ROW;

END TPX_ALARM_FILE;
/
