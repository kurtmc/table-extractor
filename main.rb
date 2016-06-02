require 'pg'
require 'pg_query'

require './insert_statement'
require './table_node'
require './sql_utils'
require './dependency'

def add_values(schema, table, t, query = nil)

    if query.nil?
        query = "SELECT * FROM #{schema}.#{table} LIMIT 1"
    end

    add_values_rec(schema, table, t, query)
end

# TODO, modify to accept mutlipe rows
# Just interate over 'values' and run query for each and append
def add_values_rec(schema, table, t, query)
    if t.parent == nil
        t.data['values'] = exec(query)
    else
        t.parent.data['values'].each_with_index { |v, i|
            where = "WHERE"
            unless t.parent.foreign_keys.size == 0
                t.parent.foreign_keys.each { |x|
                    if x['foreign_table_name'] == t.table_name
                        foreign_col = x['foreign_column_name']
                        col = x['column_name']
                        parent_val = t.parent.data['values'][i][col]
                        where = "#{where} #{foreign_col} = '#{parent_val}'"
                    end
                }
            end
            query = "SELECT * FROM #{schema}.#{table} #{where} LIMIT 1";
            if t.data['values'].nil?
                t.data['values'] = Array.new
            end
            t.data['values'] << exec(query)[0]
        }
    end

    t.depends.each { |n|
        add_values_rec(schema, n.table_name, n, query)
    }
end

def retrive_columns(schema, table)
    q = "
    SELECT *
    FROM information_schema.columns
    WHERE table_schema = '#{schema}'
      AND table_name   = '#{table}'
    "
    res = exec(q)
    result = Array.new
    res.each { |x|
        result << x['column_name']
    }
    return result
end

def generate_insert(schema, table_node, inserts)

    table_node.depends.each { |t|
        generate_insert(schema, t, inserts)
    }

    insert = InsertStatement.new

    insert.schema = schema
    insert.table_name = table_node.table_name
    columns = retrive_columns(schema, table_node.table_name)
    insert.columns = columns
    values = Array.new
    table_node.data['values'].each_with_index { |v, i|
        values << Array.new
        columns.each { |col|
            values[i] << table_node.data['values'][i][col]
        }
    }
    values.each { |row|
        row.each_with_index { |v, i|
            if row[i] == nil
                row[i] = 'NULL'
            else row[i] = "'#{row[i]}'"
            end
        }
    }

    insert.values = values
    inserts << insert
end

def print_inserts(inserts)
    inserts.each { |i|
        puts "do $$"
        puts "begin"
        puts "INSERT INTO #{i.schema}.#{i.table_name}"
        puts "(#{i.columns.join(", ")})"
        puts "VALUES"
        i.values.each_with_index { |v, index|
            if index == i.values.size - 1
                puts "(#{v.join(", ")})"
            else
                puts "(#{v.join(", ")}),"
            end
        }
        puts ';'
        puts "exception when unique_violation then"
        puts "raise notice 'Did not insert, since unique_violation';"
        puts "end $$;"
    }
end

def pretty_print(table_node, indent = "")
    if table_node.column_name == nil
        puts "#{indent}#{table_node.table_name}" 
        puts "Values: #{table_node.values.inspect}"
    else
        puts "#{indent}#{table_node.table_name}: #{table_node.foreign_column_name} -> #{table_node.column_name}" 
    end
    table_node.depends.each { |x|
        pretty_print(x, indent + "    ")
    }
end

def get_inserts_for_table(schema, table)
    dependency_tree = foreign_key_tree(schema, table)

    add_values(schema, table, dependency_tree)

    inserts = Array.new
    generate_insert(schema, dependency_tree, inserts)

    # TODO may not ever need this. Possibly have to check that
    # inserts = inserts.uniq

    print_inserts(inserts)
end

def get_inserts_for_query(query)
    ast = PgQuery.parse(query)
    
    if ast.parsetree.size == 0
        puts 'No querys parsed'
        exit 1
    end

    if ast.parsetree.size > 1
        puts 'One one query can be processed'
        exit 1
    end

    querytree = ast.parsetree[0]

    selectTree = querytree['SELECT'] 

    if selectTree.nil?
        puts 'Can only process SELECT queries'
        exit 1
    end

    if selectTree['limitCount'].nil? || selectTree['limitCount']['A_CONST']['val'] > 100
        puts 'You must put a limit on your query less than 100'
        puts 'This tool generates insert querys, a large output will cause huge amounts of text to be produced'
        exit 1
    end

    limit = selectTree['limitCount']['A_CONST']['val']
    schema = selectTree['fromClause'][0]['RANGEVAR']['schemaname']
    table = selectTree['fromClause'][0]['RANGEVAR']['relname']

    query = "SELECT * FROM #{schema}.#{table} LIMIT #{limit}"

    puts "Query: #{query}"

    dependency_tree = foreign_key_tree(schema, table)

    add_values(schema, table, dependency_tree, query)

    inserts = Array.new
    generate_insert(schema, dependency_tree, inserts)

    # TODO may not ever need this. Possibly have to check that
    # inserts = inserts.uniq

    print_inserts(inserts)
    
end
