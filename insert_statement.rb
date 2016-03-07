class InsertStatement
    attr_accessor :schema
    attr_accessor :table_name
    attr_accessor :columns
    attr_accessor :values

    def ==(o)
        unless o.class == self.class
            return false
        end

        unless o.schema == self.schema
            return false
        end

        unless o.table_name == self.table_name
            return false
        end

        unless o.columns.size == self.columns.size
            return false
        end

        o.columns.each_with_index { |x, i|
            unless o.columns[i] == self.columns[i]
                return false
            end
        }

        unless o.values.size == self.values.size
            return false
        end

        o.values.each_with_index { |x, i|
            unless o.values[i] == self.values[i]
                return false
            end
        }

        return true
    end
    alias :eql? :==
end
