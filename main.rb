#!/usr/bin/env ruby
require 'pg'

class TableNode
    attr_accessor :table_name
    attr_accessor :column_name
    attr_accessor :foreign_column_name
    attr_accessor :depends
    attr_accessor :parent
end

def exec(query)
    conn = PG.connect(host: 'db.local.eroad.io', dbname: 'central', user: 'postgres', password: 'postgres', port: '5432')
    result = Array.new
    conn.exec(query).each do |row|
        result << row
    end
    return result
end

def foreign_keys(table)
    q = "
    SELECT DISTINCT
        tc.constraint_name, tc.table_name, kcu.column_name, 
        ccu.table_name AS foreign_table_name,
        ccu.column_name AS foreign_column_name 
    FROM 
        information_schema.table_constraints AS tc 
        JOIN information_schema.key_column_usage AS kcu
        ON tc.constraint_name = kcu.constraint_name
        JOIN information_schema.constraint_column_usage AS ccu
        ON ccu.constraint_name = tc.constraint_name
        WHERE constraint_type = 'FOREIGN KEY' AND tc.table_name='#{table}';
    "
    return exec(q)
end

def foreign_key_tree(table, parent = nil, foreign_column = nil, column = nil)
    t = TableNode.new
    t.table_name = table
    t.foreign_column_name = foreign_column
    t.column_name = column
    t.parent = parent
    t.depends = Array.new

    x = foreign_keys(table)
    
    x.each do |dep|
        new_table = foreign_key_tree(dep['foreign_table_name'], t, dep['foreign_column_name'], dep['column_name'])
        t.depends << new_table
    end

    return t
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

def generate_insert(schema, table_node)
    table_node.depends.each { |t|
        generate_insert(schema, t)
    }

    puts "INSERT INTO #{schema}.#{table_node.table_name}"
    puts "(#{retrive_columns(schema, table_node.table_name).join(", ")})"
    puts "VALUES()"

end

def pretty_print(table_node, indent = "")
    if table_node.column_name == nil
        puts "#{indent}#{table_node.table_name}" 
    else
        puts "#{indent}#{table_node.table_name}: #{table_node.foreign_column_name} -> #{table_node.column_name}" 
    end
    table_node.depends.each { |x|
        pretty_print(x, indent + "    ")
    }
end

dependency_tree = foreign_key_tree("#{ARGV[0]}")

pretty_print(dependency_tree)

generate_insert('central', dependency_tree)
