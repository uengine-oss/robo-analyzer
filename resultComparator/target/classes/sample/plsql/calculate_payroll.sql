CREATE OR REPLACE PROCEDURE calculate_payroll (
    p_employeeid IN NUMBER,
    p_include_overtime IN NUMBER,
    p_include_unpaid_leave IN NUMBER,
    p_include_tax IN NUMBER
) AS
    v_overtime_hours NUMBER;
    v_overtime_rate NUMBER := 1.5;
    v_overtime_pay NUMBER := 0;
    v_unpaid_leave_days NUMBER;
    v_unpaid_deduction NUMBER := 0;
    v_tax_rate NUMBER := 0.1;
    v_contract_tax_rate NUMBER;
    v_tax_deduction NUMBER := 0;
    v_base_salary NUMBER;
    v_employee_type VARCHAR2(255);
BEGIN
    SELECT BASESALARY, EMPLOYEETYPE, CONTRACTTAXRATE
    INTO v_base_salary, v_employee_type, v_contract_tax_rate
    FROM EMPLOYEES
    WHERE EMPLOYEEID = p_employeeid;

    IF p_include_overtime = 1 THEN
        SELECT NVL(SUM(OVERHOURS), 0)
        INTO v_overtime_hours
        FROM WORK_LOGS
        WHERE EMPLOYEEID = p_employeeid
          AND WORKDATE BETWEEN TRUNC(SYSDATE, 'MM') AND LAST_DAY(SYSDATE);

        v_overtime_pay := v_overtime_hours * (v_base_salary / 160) * v_overtime_rate;
    END IF;

    IF p_include_unpaid_leave = 1 THEN
        SELECT NVL(SUM(LEAVEDAYS), 0)
        INTO v_unpaid_leave_days
        FROM LEAVE_RECORDS
        WHERE EMPLOYEEID = p_employeeid
          AND LEAVETYPE = 'Unpaid'
          AND LEAVEDATE BETWEEN TRUNC(SYSDATE, 'MM') AND LAST_DAY(SYSDATE);

        v_unpaid_deduction := (v_base_salary / 20) * v_unpaid_leave_days;
    END IF;

    IF p_include_tax = 1 THEN
        IF v_employee_type = 'Contract' THEN
            v_tax_rate := v_contract_tax_rate;
        END IF;

        v_tax_deduction := (v_base_salary + v_overtime_pay - v_unpaid_deduction) * v_tax_rate;
    END IF;

    UPDATE EMPLOYEES
    SET FINALSALARY = v_base_salary + v_overtime_pay - v_unpaid_deduction - v_tax_deduction
    WHERE EMPLOYEEID = p_employeeid;

    COMMIT;
END calculate_payroll;