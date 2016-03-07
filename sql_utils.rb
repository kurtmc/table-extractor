def exec(query)
    conn = PG.connect(host: 'db.local.eroad.io', dbname: 'central', user: 'postgres', password: 'postgres', port: '5432')
    result = Array.new
    conn.exec(query).each do |row|
        result << row
    end
    return result
end
