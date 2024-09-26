import pl from 'nodejs-polars';

export const loadData = (filePath) => {
    const df = pl.readJSON(filePath, { format: "lines", inferSchemaLength: 0 });
    
    // Rename columns to avoid column name duplication in later stages
    return df.rename(
        { "id": "_id", "created_at": "_created_at"}
    ).unnest('actor');
};
