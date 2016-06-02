require 'yaml'

def exec(query)
    db = YAML.load_file('database.yml')

    conn = PG.connect(host: db['host'], dbname: 'central', user: db['username'], password: db['password'], port: '5432')
    result = Array.new
    conn.exec(query).each do |row|
        result << row
    end
    return result
end
