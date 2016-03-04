#!/usr/bin/env ruby
require 'pg'

class TableNode
    attr_accessor :table_name
    attr_accessor :column_name
    attr_accessor :depends
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
    SELECT
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

def foreign_key_tree(table)
    t = TableNode.new
    t.table_name = table
    t.column_name = nil
    t.depends = Array.new

    x = foreign_keys(table)
    puts x.inspect
    x.each do |dep|
        new_table = TableNode.new
        new_table.table_name = dep['foreign_table_name']
        new_table.column_name = dep['foreign_column_name']
        t.depends << new_table
    end

    return t
end

def generate_insert(table)

end

puts foreign_key_tree('logbook_entry_note').inspect
