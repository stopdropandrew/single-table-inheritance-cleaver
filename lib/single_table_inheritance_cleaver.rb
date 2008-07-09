class SingleTableInheritanceCleaver
  attr_accessor :source, :destinations, :chunk_size, :rejections, :conditions, :excluded_types

  DISALLOWED_COLUMN_NAMES = %w(id type)

  def initialize(source, options = {})
    self.source = source

    self.rejections = options[:rejections] || {}
    self.conditions = options[:conditions] || {}
    self.chunk_size = options[:chunk_size] || 500
    self.excluded_types = options[:excluded_types] || []
    self.destinations = options[:destinations] || {}

    conflicting_types = self.destinations.keys & self.excluded_types
    raise ArgumentError, "The #{conflicting_types.join(', ')} types were explicitly included and excluded, make up your mind." unless conflicting_types.blank?

    all_types = source.find(:all, :select => 'DISTINCT type').map {|t| t.attributes['type']}
    valid_types = all_types - self.excluded_types
    valid_types.each do |type|
      self.destinations[type] ||= type.tableize
    end

    
  end
  
  # Process records from the source table into the destination tables
  def cleave!
    destinations.each do |source_type, destination_table_name|
      cleave_destination source_type, destination_table_name
    end
  end

  def cleave_destination source_type, destination_table_name, offset = 0
    while (keep_going = cleave_chunk(source_type, destination_table_name, offset))
      offset += chunk_size
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
