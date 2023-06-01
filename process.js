const cassandra = require('cassandra-driver');

// Create a connection to your Cassandra cluster
const client = new cassandra.Client({
    cloud: {
        secureConnectBundle: process.env.SECURE_CONNECT,
    },
    credentials: {
        username: process.env.CLIENT_ID,
        password: process.env.CLIENT_SECRET,
    },
    keyspace: process.env.KEYSPACE
});

// Function to perform the validations and insert into TDS_STATEMENT table
async function insertIntoTDSStatement(tdsStatementData) {
  try {
    // Perform validation 1: TDS_STATEMENT.PAN_NO exists in PAN.PAN_NO
    const panExistsQuery = 'SELECT PAN_NO FROM PAN WHERE PAN_NO = ?';
    const panExistsResult = await client.execute(panExistsQuery, [tdsStatementData.PAN_NO]);
    if (panExistsResult.rowLength === 0) {
      throw new Error(`PAN_NO '${tdsStatementData.PAN_NO}' does not exist in PAN table.`);
    }
    console.log('PAN Validation passed!')
    // Perform validation 2: TDS_STATEMENT.TAN_NO exists in TAN.TAN_NO
    const tanExistsQuery = 'SELECT TAN_NO FROM TAN WHERE TAN_NO = ?';
    const tanExistsResult = await client.execute(tanExistsQuery, [tdsStatementData.TAN_NO]);
    if (tanExistsResult.rowLength === 0) {
      throw new Error(`TAN_NO '${tdsStatementData.TAN_NO}' does not exist in TAN table.`);
    }
    console.log('TAN Validation passed!')

    // Perform validation 3: Fetch TDS_RATES.SECTION_CD and TDS_RATES.TDS_RATE for TDS_STATEMENT.SECTION_CODE
    const tdsRatesQuery = `SELECT FY, SECTION_CD, TDS_RATE FROM TDS_RATES WHERE FY = ${tdsStatementData.FY} and SECTION_CD = '${tdsStatementData.SECTION_CODE}'`;
    const tdsRatesResult = await client.execute(tdsRatesQuery);
    if (tdsRatesResult.rowLength === 0) {
      throw new Error(`SECTION_CODE '${tdsStatementData.SECTION_CODE}' does not exist in TDS_RATES table.`);
    }
    console.log('TDS Section Code Validation passed!')

    const tdsRate = tdsRatesResult.rows[0].tds_rate;    
    // Check if TDS_STATEMENT.TDS_AMT is equal to TDS_STATEMENT.PMT_AMT x TDS_RATES.TDS_RATE
    const calculatedTDSAmt = tdsStatementData.PMT_AMT * tdsRate / 100;    
    if (calculatedTDSAmt !== tdsStatementData.TDS_AMT) {
      throw new Error(`TDS_AMT '${tdsStatementData.TDS_AMT}' does not match the calculated value based on TDS_RATE.`);
    }
    console.log('TDS Calculation Validation passed!')

    // Perform validation 4: Fetch OLTAS.CIN and OLTAS.AMOUNT for TDS_STATEMENT.CIN_NO
    const oltasDataQuery = `SELECT TAN_NO, FY, CIN, AMOUNT FROM OLTAS WHERE FY=${tdsStatementData.FY} and CIN = '${tdsStatementData.CIN_NO}' and TAN_NO='${tdsStatementData.TAN_NO}'`;
    const oltasDataResult = await client.execute(oltasDataQuery);
    if (oltasDataResult.rowLength === 0) {
      throw new Error(`CIN_NO '${tdsStatementData.CIN_NO}' does not exist in OLTAS table.`);
    }
    const oltasAmount = oltasDataResult.rows[0].amount;
    console.log("OLTAS Amount=", Number(oltasAmount));

    // Calculate the sum of all TDS_STATEMENT.TDS_AMT for the given CIN_NO and TAN_NO
    const tdsAmtSumQuery = `SELECT SUM(TDS_AMT) AS total FROM TDS_STATEMENT WHERE FY=${tdsStatementData.FY} and CIN_NO = '${tdsStatementData.CIN_NO}' and TAN_NO='${tdsStatementData.TAN_NO}' and PAN_NO='${tdsStatementData.PAN_NO}'`;
    const tdsAmtSumResult = await client.execute(tdsAmtSumQuery);    
    const tdsAmtSum = Number(tdsAmtSumResult.rows[0].total || 0);    
    // Check if the sum of TDS_STATEMENT.TDS_AMT exceeds OLTAS.AMOUNT
    if (tdsAmtSum + tdsStatementData.TDS_AMT > oltasAmount) {
        throw new Error(`Sum of TDS_AMT exceeds the available OLTAS amount for CIN_NO '${tdsStatementData.CIN_NO}' and TAN_NO '${tdsStatementData.TAN_NO}'.`);
    }

    // If all validations pass, insert the data into the TDS_STATEMENT table
    const insertQuery = 
        `
            INSERT INTO TDS_STATEMENT (TDS_STMT_NO, FY, PERIOD, FORM, FILING_DATE, TAN_NO, DD_NO, PAN_NO, PMT_DATE, PMT_AMT, SECTION_CODE, TDS_AMT, CIN_NO) 
                VALUES (${tdsStatementData.TDS_STMT_NO}, 
                    ${tdsStatementData.FY}, 
                    '${tdsStatementData.PERIOD}',
                    '${tdsStatementData.FORM}',
                    '${tdsStatementData.FILING_DATE}',
                    '${tdsStatementData.TAN_NO}',
                    ${tdsStatementData.DD_NO},
                    '${tdsStatementData.PAN_NO}',
                    '${tdsStatementData.PMT_DATE}',
                    ${tdsStatementData.PMT_AMT},
                    '${tdsStatementData.SECTION_CODE}',
                    ${tdsStatementData.TDS_AMT},
                    '${tdsStatementData.CIN_NO}'
                    );
            `;    
    await client.execute(insertQuery);
    console.log('Data inserted into TDS_STATEMENT successfully.');
    }
    catch(e){
        console.log(e);
    }
    
}


statements = [
    {
      "TDS_STMT_NO": 1001,
      "FY": 2022,
      "PERIOD": "Q1",
      "FORM": "26Q",
      "FILING_DATE": "15-Jul-22",
      "TAN_NO": "TAN0001",
      "DD_NO": 1,
      "PAN_NO": "PAN0001",
      "PMT_DATE": "15-Apr-22",
      "PMT_AMT": 744302,
      "SECTION_CODE": "TDS-194A",
      "TDS_AMT": 74430.2,
      "CIN_NO": "5001"
    },
    {
      "TDS_STMT_NO": 1001,
      "FY": 2022,
      "PERIOD": "Q1",
      "FORM": "26Q",
      "FILING_DATE": "15-Jul-22",
      "TAN_NO": "TAN0001",
      "DD_NO": 2,
      "PAN_NO": "PAN0002",
      "PMT_DATE": "16-Apr-22",
      "PMT_AMT": 60723,
      "SECTION_CODE": "TDS-194B",
      "TDS_AMT": 6072.3,
      "CIN_NO": "5001"
    },
    {
      "TDS_STMT_NO": 1001,
      "FY": 2022,
      "PERIOD": "Q1",
      "FORM": "26Q",
      "FILING_DATE": "15-Jul-22",
      "TAN_NO": "TAN0001",
      "DD_NO": 3,
      "PAN_NO": "PAN0003",
      "PMT_DATE": "17-Apr-22",
      "PMT_AMT": 352038,
      "SECTION_CODE": "TDS-194C",
      "TDS_AMT": 35203.8,
      "CIN_NO": "5001"
    }
  ]
  

async function insert(i){
    try{
        await insertIntoTDSStatement(statements[i]);
    }catch (e){
        console.log(e);
    }
}

try{
    insert(2);
}catch(e){
    console.log(e)
}