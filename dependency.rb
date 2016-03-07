def foreign_keys(schema, table)
    q = "
SELECT 
    att2.attname as \"column_name\", 
    cl.relname as \"foreign_table_name\", 
    att.attname as \"foreign_column_name\"
from
   (select 
        unnest(con1.conkey) as \"parent\", 
        unnest(con1.confkey) as \"child\", 
        con1.confrelid, 
        con1.conrelid
    from 
        pg_class cl
        join pg_namespace ns on cl.relnamespace = ns.oid
        join pg_constraint con1 on con1.conrelid = cl.oid
    where
        cl.relname = '#{table}'
        and ns.nspname = '#{schema}'
        and con1.contype = 'f'
   ) con
   join pg_attribute att on
       att.attrelid = con.confrelid and att.attnum = con.child
   join pg_class cl on
       cl.oid = con.confrelid
   join pg_attribute att2 on
       att2.attrelid = con.conrelid and att2.attnum = con.parent
    "
    return exec(q)
end

def foreign_key_tree(schema, table, parent = nil, foreign_column = nil, column = nil)
    t = TableNode.new
    t.data = Hash.new
    t.table_name = table
    t.foreign_column_name = foreign_column
    t.column_name = column
    t.parent = parent
    t.depends = Array.new
    t.foreign_keys = foreign_keys(schema, table)

    t.foreign_keys.each do |dep|
        new_table = foreign_key_tree(schema, dep['foreign_table_name'], t, dep['foreign_column_name'], dep['column_name'])
        t.depends << new_table
    end

    return t
end
