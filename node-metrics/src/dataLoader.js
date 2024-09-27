import pl from 'nodejs-polars';

export const loadData = (filePath) => {
    console.log(`Reading input file ${filePath}`);
    
    const df = pl.readJSON(filePath, { format: "lines", inferSchemaLength: 0 });
    return df;
};
