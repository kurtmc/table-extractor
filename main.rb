require 'pg'
require 'pg_query'

require './insert_statement'
require './table_node'
require './sql_utils'
require './dependency'

# TODO, modify to accept mutlipe rows
# Just interate over 'values' and run query for each and append
def add_values(schema, table, t)
    if t.parent == nil
        t.data['values'] = exec("SELECT * FROM #{schema}.#{table} LIMIT 1")[0]
    else
        where = "WHERE"
        unless t.parent.foreign_keys.size == 0
            t.parent.foreign_keys.each { |x|
                if x['foreign_table_name'] == t.table_name
                    foreign_col = x['foreign_column_name']
                    col = x['column_name']
                    parent_val = t.parent.data['values'][col]
                    where = "#{where} #{foreign_col} = '#{parent_val}'"
                end
            }
        end
        query = "SELECT * FROM #{schema}.#{table} #{where}";
        t.data['values'] = exec(query)[0]
    end

    t.depends.each { |n|
        add_values(schema, n.table_name, n)
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
    columns.each { |col|
        values << table_node.data['values'][col]
    }
    values.each_with_index { |val, i|
        if values[i] == nil
            values[i] = 'NULL'
        else values[i] = "'#{values[i]}'"
        end
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
        puts "VALUES(#{i.values.join(", ")});"
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

    puts selectTree['limitCount'].inspect

end
