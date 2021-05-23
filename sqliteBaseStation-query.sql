SELECT ModeS, Registration,  ICAOTypeCode, SUBSTR(REPLACE(REPLACE(OperatorFlagCode,Registration,''),ICAOTypeCode,''),1,3) AS Operator
FROM Aircraft;