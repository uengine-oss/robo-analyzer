CREATE OR REPLACE PACKAGE TPX_ALARM AS

ROW TPJ_ALARM%ROWTYPE;
FLAG VARCHAR2(1) := 'N';

PROCEDURE SET_KEY (
     iAlarmKey     IN TPJ_ALARM.PROJ_ALARM_KEY%TYPE
);

PROCEDURE INPUT (
    iRow        IN TPJ_ALARM%ROWTYPE
);

PROCEDURE INS_ROW (
    iMailType   	IN 	TPJ_ALARM.MAIL_TYPE  	%TYPE,
    iTitle  		IN 	TPJ_ALARM.TITLE  	  	%TYPE,
    iSender  		IN 	TPJ_ALARM.SENDER  	  	%TYPE,
    iSendFlag  		IN 	TPJ_ALARM.SEND_FLAG   	%TYPE,
    iEffectDate  	IN 	TPJ_ALARM.EFFECT_DATE 	%TYPE,
    iExpiredDate  	IN 	TPJ_ALARM.EXPIRED_DATE	%TYPE,
    iUserKey      	IN 	TPJ_ALARM.USER_KEY     	%TYPE,
    iProjKey      	IN 	TPJ_ALARM.PROJ_KEY     	%TYPE,
    iSiteKey      	IN 	TPJ_ALARM.SITE_KEY     	%TYPE,
 	oAlarmKey  	 	OUT TPJ_ALARM.PROJ_ALARM_KEY%TYPE
);

PROCEDURE UPD_SEND_FLAG (
	iAlarmKey  	 	IN  TPJ_ALARM.PROJ_ALARM_KEY%TYPE,
    iSendFlag  		IN 	TPJ_ALARM.SEND_FLAG   	%TYPE
);


END TPX_ALARM;
/





CREATE OR REPLACE PACKAGE BODY TPX_ALARM AS

FUNCTION p_EQUAL (
    iRow    IN TPJ_ALARM%ROWTYPE
)
RETURN BOOLEAN IS
    vBool   BOOLEAN;
BEGIN
	vBool:= ROW.PROJ_ALARM_KEY       = iRow.PROJ_ALARM_KEY      
        AND ROW.MAIL_TYPE            = iRow.MAIL_TYPE          
        AND ROW.TITLE                = iRow.TITLE               
        AND ROW.SENDER               = iRow.SENDER              
        AND ROW.SEND_FLAG            = iRow.SEND_FLAG           
        AND ROW.EFFECT_DATE          = iRow.EFFECT_DATE         
        AND ROW.EXPIRED_DATE         = iRow.EXPIRED_DATE        
        AND ROW.DEL_FLAG             = iRow.DEL_FLAG            
        AND ROW.PROJ_KEY             = iRow.PROJ_KEY
        AND ROW.SITE_KEY             = iRow.SITE_KEY;
    
    RETURN vBool;
END p_EQUAL;

FUNCTION p_VALUE (
    iRow    IN TPJ_ALARM%ROWTYPE
)
RETURN VARCHAR2 IS
BEGIN
    RETURN
        'PROJ_ALARM_KEY       : ['||ROW.PROJ_ALARM_KEY      ||'] => ['||iRow.PROJ_ALARM_KEY      ||']'||CHR(10)||
        'MAIL_TYPE            : ['||ROW.MAIL_TYPE           ||'] => ['||iRow.MAIL_TYPE           ||']'||CHR(10)||
        'TITLE                : ['||ROW.TITLE               ||'] => ['||iRow.TITLE               ||']'||CHR(10)||
        'SENDER               : ['||ROW.SENDER              ||'] => ['||iRow.SENDER              ||']'||CHR(10)||
        'SEND_FLAG            : ['||ROW.SEND_FLAG           ||'] => ['||iRow.SEND_FLAG           ||']'||CHR(10)||
        'EFFECT_DATE          : ['||ROW.EFFECT_DATE         ||'] => ['||iRow.EFFECT_DATE         ||']'||CHR(10)||
        'EXPIRED_DATE         : ['||ROW.EXPIRED_DATE        ||'] => ['||iRow.EXPIRED_DATE        ||']'||CHR(10)||
        'DEL_FLAG             : ['||ROW.DEL_FLAG            ||'] => ['||iRow.DEL_FLAG            ||']'||CHR(10)||
        'INPUT_TIME           : ['||ROW.INPUT_TIME          ||'] => ['||iRow.INPUT_TIME          ||']'||CHR(10)||
        'USER_KEY             : ['||ROW.USER_KEY            ||'] => ['||iRow.USER_KEY            ||']'||CHR(10)||
        'PROJ_KEY             : ['||ROW.PROJ_KEY            ||'] => ['||iRow.PROJ_KEY            ||']'||CHR(10)||
        'SITE_KEY             : ['||ROW.SITE_KEY            ||'] => ['||iRow.SITE_KEY            ||']'||CHR(10)||
        '';
END p_VALUE;

PROCEDURE p_INSERT (
    iRow    IN TPJ_ALARM%ROWTYPE
) IS
BEGIN
    BEGIN
        INSERT INTO TPJ_ALARM VALUES iRow;
    EXCEPTION WHEN OTHERS THEN
        RAISE_APPLICATION_ERROR(-20102, SQLERRM||CHR(10)||
            'CRS_ERROR: Cannot INSERT into the Table[TPJ_ALARM]'||CHR(10)||p_VALUE(iRow)||
            $$PLSQL_UNIT||'.'||$$PLSQL_LINE);
    END;
END p_INSERT;

PROCEDURE p_UPDATE (
    iRow    IN TPJ_ALARM%ROWTYPE
) IS
BEGIN
    BEGIN
        UPDATE TPJ_ALARM SET ROW = iRow
        WHERE PROJ_ALARM_KEY = iRow.PROJ_ALARM_KEY;
    EXCEPTION WHEN OTHERS THEN
        RAISE_APPLICATION_ERROR(-20102, SQLERRM||CHR(10)||
            'CRS_ERROR: Cannot UPDATE into the Table[TPJ_ALARM]'||CHR(10)||p_VALUE(iRow)||
            $$PLSQL_UNIT||'.'||$$PLSQL_LINE);
    END;
END p_UPDATE;

FUNCTION p_GET_ROW (
     iAlarmKey     IN TPJ_ALARM.PROJ_ALARM_KEY%TYPE
)
RETURN TPJ_ALARM%ROWTYPE IS
    vRow TPJ_ALARM%ROWTYPE;
BEGIN
    SELECT * INTO vRow FROM TPJ_ALARM
    WHERE PROJ_ALARM_KEY = iAlarmKey;
    
    RETURN vRow;
END p_GET_ROW;


PROCEDURE SET_KEY (
     iAlarmKey     IN TPJ_ALARM.PROJ_ALARM_KEY%TYPE
) IS
BEGIN
    ROW := p_GET_ROW(iAlarmKey);
END SET_KEY;


PROCEDURE INPUT (
    iRow        IN TPJ_ALARM%ROWTYPE
)IS
    vRow        TPJ_ALARM   %ROWTYPE;
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
    iMailType   	IN 	TPJ_ALARM.MAIL_TYPE  	%TYPE,
    iTitle  		IN 	TPJ_ALARM.TITLE  	  	%TYPE,
    iSender  		IN 	TPJ_ALARM.SENDER  	  	%TYPE,
    iSendFlag  		IN 	TPJ_ALARM.SEND_FLAG   	%TYPE,
    iEffectDate  	IN 	TPJ_ALARM.EFFECT_DATE 	%TYPE,
    iExpiredDate  	IN 	TPJ_ALARM.EXPIRED_DATE	%TYPE,
    iUserKey      	IN 	TPJ_ALARM.USER_KEY     	%TYPE,
    iProjKey      	IN 	TPJ_ALARM.PROJ_KEY     	%TYPE,
    iSiteKey      	IN 	TPJ_ALARM.SITE_KEY     	%TYPE,
 	oAlarmKey  	 	OUT TPJ_ALARM.PROJ_ALARM_KEY%TYPE
)
IS
    vRow   TPJ_ALARM%ROWTYPE;
BEGIN
	oAlarmKey := SEQ_PROJ_ALARM_KEY.NEXTVAL;
	
    vRow.PROJ_ALARM_KEY	:= oAlarmKey;
    vRow.MAIL_TYPE      := iMailType;
    vRow.TITLE         	:= iTitle;
    vRow.SENDER         := iSender;
    vRow.SEND_FLAG      := iSendFlag;
    vRow.EFFECT_DATE    := NVL(iEffectDate, TO_CHAR(SYSDATE, 'YYYY-MM-DD'));
    vRow.EXPIRED_DATE   := NVL(iExpiredDate, TO_CHAR(SYSDATE, 'YYYY-MM-DD'));
    vRow.DEL_FLAG       := 'N';
    vRow.INPUT_TIME     := SYSDATE;
    vRow.USER_KEY       := iUserKey;
    vRow.PROJ_KEY       := iProjKey;
    vRow.SITE_KEY       := iSiteKey;
    
    INPUT(vRow);
END INS_ROW;

PROCEDURE UPD_SEND_FLAG (
	iAlarmKey  	 	IN  TPJ_ALARM.PROJ_ALARM_KEY%TYPE,
    iSendFlag  		IN 	TPJ_ALARM.SEND_FLAG   	%TYPE
) IS
BEGIN
	UPDATE TPJ_ALARM
	SET SEND_FLAG  = iSendFlag
	  , INPUT_TIME = SYSDATE
	WHERE PROJ_ALARM_KEY = iAlarmKey
	  AND SEND_FLAG     != iSendFlag;
END UPD_SEND_FLAG;

END TPX_ALARM;
/