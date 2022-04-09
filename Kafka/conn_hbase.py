import happybase

def connect_to_hbase(hostname, port, namespace, table_name, batch_size = 1000):
    """ Connect to HBase server.
    This will use the host, namespace, table name, and batch size as defined in
    the global variables above.
    """
    conn = happybase.Connection(host = hostname,
        port=port,
        table_prefix = namespace,
        table_prefix_separator = ":")
    conn.open()
    table = conn.table(table_name)
    batch = table.batch(batch_size = batch_size)
    return conn, table, batch

def insert_row(batch, row):
    """ Insert a row into HBase.
    Write the row to the batch. When the batch size is reached, rows will be
    sent to the database.
    Rows have the following schema:
        [ id, keyword, subcategory, type, township, city, zip, council_district,
          opened, closed, status, origin, location ]
    """
    batch.put(row[0], { b'u:u_id': row[1], b'u:u_se': row[2], b't:ed': row[3],
        b't:et': row[4], b'pd:et': row[5], b'pd:pi': row[6],
        b'pd:ci': row[7], b'pd:cc': row[8], b'pd:br': row[9],
        b'pd.pr': row[10]})
