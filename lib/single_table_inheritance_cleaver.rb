class SingleTableInheritanceCleaver
  attr_accessor :source, :destinations, :chunk_size, :rejections, :conditions

  DISALLOWED_COLUMN_NAMES = %w(id type)

  def initialize(source, options = {})
    self.source = source
    
    all_types = source.find(:all, :select => 'DISTINCT type').map {|t| t.attributes['type']}
    self.destinations = options[:destinations] || {}
    all_types.each do |type|
      self.destinations[type] ||= type.tableize
    end
    
    self.rejections = options[:rejections] || {}
    self.conditions = options[:conditions] || {}
    
    self.chunk_size = options[:chunk_size] || 50
  end
  
  # Process records from the source table into the destination tables
  def cleave!
    destinations.each do |source_type, destination_table_name|
      offset = 0
      while (keep_going = cleave_chunk(source_type, destination_table_name, offset))
        offset += chunk_size
      end
    end
  end
  
  def cleave_chunk source_type, destination_table_name, offset = 0
    previous_max = source_type.constantize.maximum('id')
    column_names = self.column_names(destination_table_name)

    conditions = source.send(:merge_conditions, {:type => source_type}, self.conditions[destination_table_name])

    latest_insert = source.connection.insert <<-SQL
      INSERT INTO #{destination_table_name}(#{column_names}) SELECT #{column_names} FROM #{source.table_name} WHERE #{conditions} LIMIT #{self.chunk_size} OFFSET #{offset}
    SQL
    current_max = source_type.constantize.maximum('id')
    
    return current_max.to_i != previous_max.to_i
  end

  def column_names(destination_table_name)
    names = self.source.column_names
    
    names.delete_if { |name| DISALLOWED_COLUMN_NAMES.include?(name) || Array(self.rejections[destination_table_name]).include?(name) }
    names.join(', ')
  end
end
